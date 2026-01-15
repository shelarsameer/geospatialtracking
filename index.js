import express from 'express';
import cors from 'cors';
import multer from 'multer';
import { Pool } from 'pg';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { parseExcel, parseCSV } from './utils/fileParser.js';
import { reconcileData } from './utils/reconciliation.js';
import { initializeDatabase } from './utils/database.js';

// Normalize values for database storage
function normalizeValueForDB(value) {
  if (value === null || value === undefined || value === '') {
    return 0;
  }

  let strValue = String(value).trim();

  if (strValue === '-') {
    return 0;
  }

  // DO NOT normalize date separators here - this causes invoice numbers like "25-26/0001" to be converted
  // The backend will handle date conversion for specific date columns only

  return strValue;
}

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const PORT = process.env.PORT || 5000;

// Middleware
app.use(cors());
// Increase body size limits since reconciliation payloads (exact matches) can exceed 100kb
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true, limit: '10mb' }));

// Database connection
const pool = new Pool({
  user: 'postgres',
  password: 'Sam@16704',
  host: 'localhost',
  port: 5432,
  database: 'gst_recon'
});

// Multer setup for file uploads
const upload = multer({ dest: 'uploads/' });

// Initialize database
await initializeDatabase(pool);

// Routes

// Health check
app.get('/api/health', (req, res) => {
  res.json({ status: 'OK', message: 'Server is running' });
});

// Upload and parse files
app.post('/api/upload', upload.fields([
  { name: 'gstFile', maxCount: 1 },
  { name: 'tallyFile', maxCount: 1 }
]), async (req, res) => {
  try {
    if (!req.files.gstFile || !req.files.tallyFile) {
      return res.status(400).json({ error: 'Both GST and Tally files are required' });
    }

    const gstFile = req.files.gstFile[0];
    const tallyFile = req.files.tallyFile[0];

    // Parse files
    let gstData = await parseFile(gstFile);
    let tallyData = await parseFile(tallyFile);

    // Get all unique column names from all rows to ensure no columns are missed
    const getAllColumns = (data) => {
      const columns = new Set();
      data.forEach(row => {
        Object.keys(row).forEach(col => columns.add(col));
      });
      return Array.from(columns);
    };

    const gstAllColumns = getAllColumns(gstData);
    const tallyAllColumns = getAllColumns(tallyData);

    // Store in database
    const uploadId = await storeUpload(pool, gstData, tallyData);

    res.json({
      success: true,
      uploadId,
      gstAllData: gstData,
      tallyAllData: tallyData,
      gstPreview: gstData.slice(1, 6),
      tallyPreview: tallyData.slice(1, 6),
      gstHeaders: gstAllColumns,
      tallyHeaders: tallyAllColumns
    });
  } catch (error) {
    console.error('Upload error:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get upload details
app.get('/api/upload/:uploadId', async (req, res) => {
  try {
    const { uploadId } = req.params;
    const result = await pool.query(
      'SELECT * FROM uploads WHERE id = $1',
      [uploadId]
    );
    
    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Upload not found' });
    }

    res.json(result.rows[0]);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Run reconciliation
app.post('/api/reconcile', async (req, res) => {
  try {
    const { uploadId, gstColumns, tallyColumns, gstHeaderRow = 1, tallyHeaderRow = 1 } = req.body;

    if (!uploadId || !gstColumns || !tallyColumns) {
      return res.status(400).json({ error: 'Missing required fields' });
    }

    // Get data from database
    const uploadResult = await pool.query(
      'SELECT gst_data, tally_data FROM uploads WHERE id = $1',
      [uploadId]
    );

    if (uploadResult.rows.length === 0) {
      return res.status(404).json({ error: 'Upload not found' });
    }

    let { gst_data, tally_data } = uploadResult.rows[0];

    // Skip rows before header row
    gst_data = gst_data.slice(gstHeaderRow - 1);
    tally_data = tally_data.slice(tallyHeaderRow - 1);

    // Run initial reconciliation (for storing in results table)
    const results = reconcileData(gst_data, tally_data, gstColumns, tallyColumns);

    // Store results with header row info
    const resultId = await storeResults(pool, uploadId, results, gstHeaderRow, tallyHeaderRow);

    try {
      // Store mapped data in actual tables
      await storeMappedData(pool, resultId, gst_data, tally_data, gstColumns, tallyColumns);

      // Perform SQL-based reconciliation on the mapped tables
      const sqlResults = await performSQLReconciliation(pool, resultId);

      res.json({
        success: true,
        resultId,
        summary: {
          totalGstRecords: gst_data.length,
          totalTallyRecords: tally_data.length,
          exactMatches: sqlResults.exactMatches.length,
          partialMatches: sqlResults.partialMatches.length,
          tallyMismatches: sqlResults.tallyMismatches.length,
          gstMismatches: sqlResults.gstMismatches.length,
          gstHeaderRow,
          tallyHeaderRow
        }
      });
    } catch (mappingError) {
      console.error('Error in mapped data storage or SQL reconciliation:', mappingError);
      // Still return success with the initial reconciliation results
      res.json({
        success: true,
        resultId,
        summary: {
          totalGstRecords: gst_data.length,
          totalTallyRecords: tally_data.length,
          exactMatches: results.exactMatches.length,
          partialMatches: results.partialMatches.length,
          tallyMismatches: results.tallyMismatches.length,
          gstMismatches: results.gstMismatches.length,
          gstHeaderRow,
          tallyHeaderRow
        }
      });
    }
  } catch (error) {
    console.error('Reconciliation error:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get reconciliation results
app.get('/api/results/:resultId', async (req, res) => {
  try {
    const { resultId } = req.params;
    const result = await pool.query(
      'SELECT * FROM reconciliation_results WHERE id = $1',
      [resultId]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Results not found' });
    }

    res.json(result.rows[0]);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Export results
app.get('/api/results/:resultId/export', async (req, res) => {
  try {
    const { resultId } = req.params;
    const result = await pool.query(
      'SELECT * FROM reconciliation_results WHERE id = $1',
      [resultId]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Results not found' });
    }

    const data = result.rows[0];
    res.json(data);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Helper functions
async function parseFile(file) {
  const ext = path.extname(file.originalname).toLowerCase();
  
  if (ext === '.xlsx' || ext === '.xls') {
    return parseExcel(file.path);
  } else if (ext === '.csv') {
    return parseCSV(file.path);
  } else {
    throw new Error('Unsupported file format');
  }
}

async function storeUpload(pool, gstData, tallyData) {
  const result = await pool.query(
    'INSERT INTO uploads (gst_data, tally_data, created_at) VALUES ($1, $2, NOW()) RETURNING id',
    [JSON.stringify(gstData), JSON.stringify(tallyData)]
  );
  return result.rows[0].id;
}

async function storeResults(pool, uploadId, results, gstHeaderRow = 1, tallyHeaderRow = 1) {
  const result = await pool.query(
    'INSERT INTO reconciliation_results (upload_id, exact_matches, partial_matches, tally_mismatches, gst_mismatches, gst_header_row, tally_header_row, created_at) VALUES ($1, $2, $3, $4, $5, $6, $7, NOW()) RETURNING id',
    [uploadId, JSON.stringify(results.exactMatches), JSON.stringify(results.partialMatches), JSON.stringify(results.tallyMismatches), JSON.stringify(results.gstMismatches), gstHeaderRow, tallyHeaderRow]
  );
  return result.rows[0].id;
}

async function performSQLReconciliation(pool, reconciliationId) {
  try {
    const gstTableName = `gst_mapped_${reconciliationId}`;
    const tallyTableName = `tally_mapped_${reconciliationId}`;

    // Get column mapping to know how many features we have
    const mappingResult = await pool.query(
      'SELECT gst_columns, tally_columns FROM column_mappings WHERE reconciliation_id = $1',
      [reconciliationId]
    );

    if (mappingResult.rows.length === 0) {
      throw new Error('Column mapping not found');
    }

    const { gst_columns, tally_columns } = mappingResult.rows[0];
    const gstCols = Array.isArray(gst_columns) ? gst_columns : JSON.parse(gst_columns);
    const tallyCols = Array.isArray(tally_columns) ? tally_columns : JSON.parse(tally_columns);
    
    const validCount = gstCols.filter((col, idx) => col && tallyCols[idx]).length;

    // Build feature column list (excluding id and created_at)
    const featureList = Array.from({ length: validCount }, (_, i) => `feature${i + 1}`).join(', ');

    // Build feature comparison string for all features
    const featureComparisons = Array.from({ length: validCount }, (_, i) => 
      `g.feature${i + 1} = t.feature${i + 1}`
    ).join(' AND ');

    // Build discrepancy detection for partial matches
    const discrepancyChecks = Array.from({ length: validCount }, (_, i) => 
      `CASE WHEN g.feature${i + 1} <> t.feature${i + 1} THEN 'Mismatch in feature${i + 1} ' ELSE '' END`
    ).join(' || ');

    // Match Type 1: Exact Matches (only feature columns, no id or created_at)
    const exactMatchesResult = await pool.query(`
      SELECT ${featureList}
      FROM ${gstTableName} g
      INNER JOIN ${tallyTableName} t
        ON ${featureComparisons}
    `);

    // Match Type 2: Partial Matches (using first 2 features as primary identifiers)
    const partialMatchesResult = await pool.query(`
      SELECT ${featureList},
        ${discrepancyChecks} AS discrepancies
      FROM ${gstTableName} g
      INNER JOIN ${tallyTableName} t
        ON g.feature1 = t.feature1
        AND g.feature2 = t.feature2
      WHERE NOT (${featureComparisons})
    `);

    // Mismatch Type 1: In Tally but Missing in GST
    const tallyMismatchesResult = await pool.query(`
      SELECT ${featureList}
      FROM ${tallyTableName} t
      LEFT JOIN ${gstTableName} g
        ON t.feature1 = g.feature1
        AND t.feature2 = g.feature2
      WHERE g.feature1 IS NULL
    `);

    // Mismatch Type 2: In GST but Missing in Tally
    const gstMismatchesResult = await pool.query(`
      SELECT ${featureList}
      FROM ${gstTableName} g
      LEFT JOIN ${tallyTableName} t
        ON g.feature1 = t.feature1
        AND g.feature2 = t.feature2
      WHERE t.feature1 IS NULL
    `);

    console.log('SQL Reconciliation Results:', {
      exactMatches: exactMatchesResult.rows.length,
      partialMatches: partialMatchesResult.rows.length,
      tallyMismatches: tallyMismatchesResult.rows.length,
      gstMismatches: gstMismatchesResult.rows.length
    });

    return {
      exactMatches: exactMatchesResult.rows,
      partialMatches: partialMatchesResult.rows,
      tallyMismatches: tallyMismatchesResult.rows,
      gstMismatches: gstMismatchesResult.rows
    };
  } catch (error) {
    console.error('SQL reconciliation error:', error);
    throw error;
  }
}

async function storeMappedData(pool, reconciliationId, gstData, tallyData, gstColumns, tallyColumns) {
  try {
    console.log('storeMappedData called with:', { reconciliationId, gstColumnsLength: gstColumns?.length, tallyColumnsLength: tallyColumns?.length });
    
    // Ensure columns are arrays
    const gstCols = Array.isArray(gstColumns) ? gstColumns : [];
    const tallyCols = Array.isArray(tallyColumns) ? tallyColumns : [];

    // Filter out empty mappings and create feature columns
    const validMappings = gstCols
      .map((gstCol, idx) => ({
        gstCol,
        tallyCol: tallyCols[idx],
        featureNum: idx + 1
      }))
      .filter(m => m.gstCol && m.tallyCol);

    console.log('Valid mappings:', validMappings.length);

    if (validMappings.length === 0) {
      throw new Error('No valid column mappings found');
    }

    // Create GST mapped data table with feature columns
    const gstTableName = `gst_mapped_${reconciliationId}`;
    const gstColumns_sql = validMappings
      .map(m => `feature${m.featureNum} TEXT`)
      .join(', ');

    await pool.query(`
      CREATE TABLE IF NOT EXISTS ${gstTableName} (
        id SERIAL PRIMARY KEY,
        ${gstColumns_sql},
        created_at TIMESTAMP DEFAULT NOW()
      )
    `);

    // Create Tally mapped data table with feature columns
    const tallyTableName = `tally_mapped_${reconciliationId}`;
    const tallyColumns_sql = validMappings
      .map(m => `feature${m.featureNum} TEXT`)
      .join(', ');

    await pool.query(`
      CREATE TABLE IF NOT EXISTS ${tallyTableName} (
        id SERIAL PRIMARY KEY,
        ${tallyColumns_sql},
        created_at TIMESTAMP DEFAULT NOW()
      )
    `);

    // Insert GST data with normalized values
    for (const row of gstData) {
      const values = validMappings.map(m => normalizeValueForDB(row[m.gstCol] || null));
      const placeholders = validMappings.map((_, i) => `$${i + 1}`).join(', ');
      const featureNames = validMappings.map(m => `feature${m.featureNum}`).join(', ');

      await pool.query(
        `INSERT INTO ${gstTableName} (${featureNames}, created_at) VALUES (${placeholders}, NOW())`,
        values
      );
    }

    // Insert Tally data with normalized values
    for (const row of tallyData) {
      const values = validMappings.map(m => normalizeValueForDB(row[m.tallyCol] || null));
      const placeholders = validMappings.map((_, i) => `$${i + 1}`).join(', ');
      const featureNames = validMappings.map(m => `feature${m.featureNum}`).join(', ');

      await pool.query(
        `INSERT INTO ${tallyTableName} (${featureNames}, created_at) VALUES (${placeholders}, NOW())`,
        values
      );
    }

    // Store column mapping metadata
    await pool.query(
      'INSERT INTO column_mappings (reconciliation_id, gst_columns, tally_columns, created_at) VALUES ($1, $2, $3, NOW())',
      [reconciliationId, JSON.stringify(gstCols), JSON.stringify(tallyCols)]
    );

    console.log(`Mapped data stored successfully in tables: ${gstTableName}, ${tallyTableName}`);
  } catch (error) {
    console.error('Error storing mapped data:', error);
    throw error;
  }
}

// Get all mapping logs
app.get('/api/mapping-logs', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM mapping_logs ORDER BY created_at DESC');
    res.json({ logs: result.rows });
  } catch (error) {
    console.error('Error fetching mapping logs:', error);
    res.status(500).json({ error: error.message });
  }
});

// Delete mapping log
app.delete('/api/mapping-logs/:logId', async (req, res) => {
  try {
    const { logId } = req.params;
    const parsedLogId = Number(logId);
    if (!Number.isInteger(parsedLogId) || parsedLogId <= 0) {
      return res.status(400).json({ error: 'Invalid logId' });
    }
    
    // Get the log to find table names
    const logResult = await pool.query('SELECT gst_table_name, tally_table_name FROM mapping_logs WHERE id = $1', [logId]);
    if (logResult.rows.length === 0) {
      return res.status(404).json({ error: 'Log not found' });
    }

    const { gst_table_name, tally_table_name } = logResult.rows[0];

    // Drop the tables
    await pool.query(`DROP TABLE IF EXISTS ${gst_table_name}`);
    await pool.query(`DROP TABLE IF EXISTS ${tally_table_name}`);

    // Drop any saved reconciliation match tables for this log
    await pool.query(`DROP TABLE IF EXISTS exact_matches_${parsedLogId}`);
    await pool.query(`DROP TABLE IF EXISTS gst_only_${parsedLogId}`);
    await pool.query(`DROP TABLE IF EXISTS tally_only_${parsedLogId}`);
    await pool.query(`DROP TABLE IF EXISTS partial_minor_${parsedLogId}`);
    await pool.query(`DROP TABLE IF EXISTS partial_major_${parsedLogId}`);

    // Delete the log
    await pool.query('DELETE FROM mapping_logs WHERE id = $1', [logId]);

    res.json({ success: true, message: 'Mapping log deleted' });
  } catch (error) {
    console.error('Error deleting mapping log:', error);
    res.status(500).json({ error: error.message });
  }
});

// Export mapping log data
app.get('/api/export-log/:logId', async (req, res) => {
  try {
    const { logId } = req.params;
    
    const logResult = await pool.query('SELECT gst_table_name, tally_table_name FROM mapping_logs WHERE id = $1', [logId]);
    if (logResult.rows.length === 0) {
      return res.status(404).json({ error: 'Log not found' });
    }

    const { gst_table_name, tally_table_name } = logResult.rows[0];

    // Get data from both tables
    const gstResult = await pool.query(`SELECT * FROM ${gst_table_name}`);
    const tallyResult = await pool.query(`SELECT * FROM ${tally_table_name}`);

    // Create CSV content
    let csvContent = 'GST 2B Data\n';
    if (gstResult.rows.length > 0) {
      const headers = Object.keys(gstResult.rows[0]).join(',');
      csvContent += headers + '\n';
      gstResult.rows.forEach(row => {
        const values = Object.values(row).map(v => `"${v}"`).join(',');
        csvContent += values + '\n';
      });
    }

    csvContent += '\n\nTally Data\n';
    if (tallyResult.rows.length > 0) {
      const headers = Object.keys(tallyResult.rows[0]).join(',');
      csvContent += headers + '\n';
      tallyResult.rows.forEach(row => {
        const values = Object.values(row).map(v => `"${v}"`).join(',');
        csvContent += values + '\n';
      });
    }

    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', `attachment; filename="mapping_log_${logId}.csv"`);
    res.send(csvContent);
  } catch (error) {
    console.error('Error exporting mapping log:', error);
    res.status(500).json({ error: error.message });
  }
});

// Preview table data
app.get('/api/preview-table/:tableName', async (req, res) => {
  try {
    const { tableName } = req.params;
    console.log('Preview request for table:', tableName);
    
    // Validate table name - allow lowercase, numbers, underscores
    if (!/^[a-z0-9_]+$/i.test(tableName)) {
      console.error('Invalid table name format:', tableName);
      return res.status(400).json({ error: 'Invalid table name' });
    }
    
    const result = await pool.query(`SELECT * FROM "${tableName}" LIMIT 10`);
    console.log('Preview data retrieved, rows:', result.rows.length);
    res.json({ data: result.rows });
  } catch (error) {
    console.error('Error previewing table:', error.message);
    res.status(500).json({ error: error.message });
  }
});

// Reconcile mapped data - Using SQL INTERSECT for perfect matches
app.post('/api/reconcile-mapped-data', async (req, res) => {
  try {
    const { logId, gstTableName, tallyTableName, gstColumns, tallyColumns } = req.body;

    if (!gstTableName || !tallyTableName) {
      return res.status(400).json({ error: 'Missing table names' });
    }

    if (!/^[a-z0-9_]+$/.test(gstTableName) || !/^[a-z0-9_]+$/.test(tallyTableName)) {
      return res.status(400).json({ error: 'Invalid table names' });
    }

    const sanitizeColumnName = (name) => {
      if (!name) return 'col_' + Date.now();
      let sanitized = name.toLowerCase().replace(/[^a-z0-9_]/g, '_').replace(/_+/g, '_').replace(/^_|_$/g, '');
      if (!sanitized) return 'col_' + Date.now();
      if (/^\d/.test(sanitized)) {
        sanitized = 'col_' + sanitized;
      }
      return sanitized;
    };

    const gstColumnNames = gstColumns.map(col => sanitizeColumnName(col));
    const tallyColumnNames = tallyColumns.map(col => sanitizeColumnName(col));

    console.log('GST columns:', gstColumnNames);
    console.log('Tally columns:', tallyColumnNames);

    // Build SELECT clause for GST table (excluding id and created_at)
    const gstSelectCols = gstColumnNames.join(', ');
    
    // Build SELECT clause for Tally table with column aliases to match GST
    const tallySelectCols = tallyColumnNames.map((col, idx) => `${col} AS ${gstColumnNames[idx]}`).join(', ');

    // Use SQL INTERSECT to find perfect matches
    const intersectQuery = `
      SELECT ${gstSelectCols} FROM ${gstTableName}
      INTERSECT
      SELECT ${tallySelectCols} FROM ${tallyTableName}
    `;

    console.log('Running INTERSECT query:', intersectQuery);
    const exactMatchesResult = await pool.query(intersectQuery);
    const exactMatches = exactMatchesResult.rows;

    console.log('Exact matches found:', exactMatches.length);

    // Get all GST data
    const gstResult = await pool.query(`SELECT * FROM ${gstTableName}`);
    const gstData = gstResult.rows;

    // Get all Tally data
    const tallyResult = await pool.query(`SELECT * FROM ${tallyTableName}`);
    const tallyData = tallyResult.rows;


    // Format exact matches with both GST and Tally data
    const formattedExactMatches = exactMatches.map(match => {
      // Find corresponding rows in GST and Tally
      const gstRow = gstData.find(row => {
        return gstColumnNames.every(col => String(row[col] || '').trim() === String(match[col] || '').trim());
      });
      
      const tallyRow = tallyData.find(row => {
        return gstColumnNames.every((col, idx) => String(row[tallyColumnNames[idx]] || '').trim() === String(match[col] || '').trim());
      });

      return {
        gst: gstRow || match,
        tally: tallyRow || match
      };
    });

    // Find partial matches (1-3 discrepancies)
    const exactMatchSet = new Set();
    exactMatches.forEach(match => {
      const key = gstColumnNames.map(col => match[col]).join('|');
      exactMatchSet.add(key);
    });

    const partialMatches = [];
    const matchedGstIds = new Set();
    const matchedTallyIds = new Set();

    // Track exact matches
    formattedExactMatches.forEach(m => {
      if (m.gst && m.gst.id) matchedGstIds.add(m.gst.id);
      if (m.tally && m.tally.id) matchedTallyIds.add(m.tally.id);
    });

    // Find partial matches for unmatched GST records
    for (const gstRow of gstData) {
      if (matchedGstIds.has(gstRow.id)) continue; // Skip already matched

      let bestMatch = null;
      let bestDiscrepancies = 999;
      let discrepancyColumns = [];

      for (const tallyRow of tallyData) {
        if (matchedTallyIds.has(tallyRow.id)) continue; // Skip already matched

        let currentDiscrepancies = 0;
        const currentDiscrepancyColumns = [];

        for (let i = 0; i < gstColumnNames.length; i++) {
          const gstCol = gstColumnNames[i];
          const tallyCol = tallyColumnNames[i];

          let gstVal = String(gstRow[gstCol] || '').trim();
          let tallyVal = String(tallyRow[tallyCol] || '').trim();

          // Normalize ISO date timestamps to just the date part
          if (/^\d{4}-\d{2}-\d{2}T/.test(gstVal)) {
            gstVal = gstVal.split('T')[0];
          }
          if (/^\d{4}-\d{2}-\d{2}T/.test(tallyVal)) {
            tallyVal = tallyVal.split('T')[0];
          }

          if (gstVal !== tallyVal) {
            currentDiscrepancies++;
            currentDiscrepancyColumns.push({
              columnIndex: i,
              gstColumn: gstColumnNames[i],
              tallyColumn: tallyColumnNames[i],
              gstValue: gstVal,
              tallyValue: tallyVal
            });
          }
        }

        // Keep matches with 1-3 discrepancies, prefer fewer discrepancies
        if (currentDiscrepancies >= 1 && currentDiscrepancies <= 3 && currentDiscrepancies < bestDiscrepancies) {
          bestMatch = tallyRow;
          bestDiscrepancies = currentDiscrepancies;
          discrepancyColumns = currentDiscrepancyColumns;
        }
      }

      if (bestMatch && bestDiscrepancies <= 3) {
        // Calculate individual monetary discrepancies for tax/value columns
        let hasLargeDiscrepancy = false;
        let maxDiscrepancy = 0;
        const taxColumns = ['taxable_value', 'col_taxable_value', 'igst', 'col_igst', 'cgst', 'col_cgst', 'sgst', 'col_sgst', 'integrated_tax', 'col_integrated_tax', 'central_tax', 'col_central_tax', 'state_ut_tax', 'col_state_ut_tax'];
        
        discrepancyColumns.forEach((disc) => {
          const colName = disc.gstColumn.toLowerCase();
          if (taxColumns.some(tc => colName.includes(tc))) {
            const gstNum = parseFloat(String(disc.gstValue).replace(/[^0-9.-]/g, '')) || 0;
            const tallyNum = parseFloat(String(disc.tallyValue).replace(/[^0-9.-]/g, '')) || 0;
            const diff = Math.abs(gstNum - tallyNum);
            maxDiscrepancy = Math.max(maxDiscrepancy, diff);
            if (diff >= 1) {
              hasLargeDiscrepancy = true;
            }
          }
        });

        partialMatches.push({
          gst: gstRow,
          tally: bestMatch,
          discrepancies: bestDiscrepancies,
          discrepancyColumns: discrepancyColumns,
          maxDiscrepancy: maxDiscrepancy,
          isMinor: !hasLargeDiscrepancy && maxDiscrepancy > 0
        });
        matchedGstIds.add(gstRow.id);
        matchedTallyIds.add(bestMatch.id);
      }
    }

    // Update GST-only and Tally-only lists (exclude partial matches)
    const gstOnlyFiltered = gstData.filter(row => !matchedGstIds.has(row.id));
    const tallyOnlyFiltered = tallyData.filter(row => !matchedTallyIds.has(row.id));

    console.log('Partial matches:', partialMatches.length);

    return res.json({
      success: true,
      logId,
      exactMatches: formattedExactMatches.length,
      partialMatches: partialMatches.length,
      gstOnly: gstOnlyFiltered.length,
      tallyOnly: tallyOnlyFiltered.length,
      details: {
        exact: formattedExactMatches,
        partial: partialMatches,
        gstOnly: gstOnlyFiltered,
        tallyOnly: tallyOnlyFiltered
      }
    });
  } catch (error) {
    console.error('Reconciliation error:', error);
    return res.status(500).json({ error: error.message });
  }
});

// Save perfectly matched records to database
app.post('/api/save-exact-matches', async (req, res) => {
  try {
    const { logId, gstColumns, exactMatches } = req.body;

    if (!logId || !gstColumns || !exactMatches || exactMatches.length === 0) {
      return res.status(400).json({ error: 'Missing required parameters' });
    }

    const parsedLogId = Number(logId);
    if (!Number.isInteger(parsedLogId) || parsedLogId <= 0) {
      return res.status(400).json({ error: 'Invalid logId' });
    }

    const matchTableName = `exact_matches_${parsedLogId}`;
    
    // Sanitize column names
    const sanitizeColumnName = (name) => {
      if (!name) return 'col';
      let sanitized = String(name).toLowerCase().replace(/[^a-z0-9_]/g, '_').replace(/_+/g, '_').replace(/^_|_$/g, '');
      if (!sanitized) sanitized = 'col';
      if (/^\d/.test(sanitized)) sanitized = 'col_' + sanitized;
      return sanitized;
    };

    const originalColumns = Array.isArray(gstColumns) ? gstColumns.filter((c) => c) : [];
    if (originalColumns.length === 0) {
      return res.status(400).json({ error: 'No GST columns provided' });
    }

    const seen = new Map();
    const sanitizedCols = originalColumns.map((orig) => {
      const base = sanitizeColumnName(orig);
      const count = (seen.get(base) || 0) + 1;
      seen.set(base, count);
      return count === 1 ? base : `${base}_${count}`;
    });

    // Create columns SQL
    const columnsSQL = sanitizedCols
      .map(col => `"${col}" TEXT`)
      .join(', ');

    console.log('Creating exact matches table:', matchTableName);
    console.log('Columns:', columnsSQL);
    console.log('GST columns:', originalColumns);

    // Create table
    await pool.query(`
      CREATE TABLE IF NOT EXISTS ${matchTableName} (
        id SERIAL PRIMARY KEY,
        ${columnsSQL},
        created_at TIMESTAMP DEFAULT NOW()
      )
    `);

    // Insert matched records
    console.log('Inserting', exactMatches.length, 'matched records');
    const placeholders = sanitizedCols.map((_, i) => `$${i + 1}`).join(', ');
    const columnNames = sanitizedCols.map((c) => `"${c}"`).join(', ');

    const insertSql = `INSERT INTO ${matchTableName} (${columnNames}, created_at) VALUES (${placeholders}, NOW())`;

    for (const match of exactMatches) {
      const gstRow = match.gst || {};
      const values = sanitizedCols.map((sanitized) => {
        let value = gstRow[sanitized] ?? null;
        if (value && typeof value === 'string' && /^\d{4}-\d{2}-\d{2}T/.test(value)) {
          value = value.split('T')[0];
        }
        return value;
      });

      await pool.query(insertSql, values);
    }

    console.log('Exact matches saved successfully');
    return res.json({
      success: true,
      logId,
      tableName: matchTableName,
      recordsCount: exactMatches.length,
      message: 'Exact matches saved successfully'
    });
  } catch (error) {
    console.error('Error saving exact matches:', error);
    return res.status(500).json({ error: error.message });
  }
});

// Save GST-only records to database
app.post('/api/save-gst-only', async (req, res) => {
  try {
    const { logId, gstColumns, gstOnlyRows } = req.body;

    if (!logId || !gstColumns || !gstOnlyRows || gstOnlyRows.length === 0) {
      return res.status(400).json({ error: 'Missing required parameters' });
    }

    const parsedLogId = Number(logId);
    if (!Number.isInteger(parsedLogId) || parsedLogId <= 0) {
      return res.status(400).json({ error: 'Invalid logId' });
    }

    const tableName = `gst_only_${parsedLogId}`;

    const sanitizeColumnName = (name) => {
      if (!name) return 'col';
      let sanitized = String(name).toLowerCase().replace(/[^a-z0-9_]/g, '_').replace(/_+/g, '_').replace(/^_|_$/g, '');
      if (!sanitized) sanitized = 'col';
      if (/^\d/.test(sanitized)) sanitized = 'col_' + sanitized;
      return sanitized;
    };

    const originalColumns = Array.isArray(gstColumns) ? gstColumns.filter((c) => c) : [];
    if (originalColumns.length === 0) {
      return res.status(400).json({ error: 'No GST columns provided' });
    }

    const seen = new Map();
    const sanitizedCols = originalColumns.map((orig) => {
      const base = sanitizeColumnName(orig);
      const count = (seen.get(base) || 0) + 1;
      seen.set(base, count);
      return count === 1 ? base : `${base}_${count}`;
    });

    const columnsSQL = sanitizedCols
      .map(col => `"${col}" TEXT`)
      .join(', ');

    await pool.query(`
      CREATE TABLE IF NOT EXISTS ${tableName} (
        id SERIAL PRIMARY KEY,
        ${columnsSQL},
        created_at TIMESTAMP DEFAULT NOW()
      )
    `);

    const placeholders = sanitizedCols.map((_, i) => `$${i + 1}`).join(', ');
    const columnNames = sanitizedCols.map((c) => `"${c}"`).join(', ');
    const insertSql = `INSERT INTO ${tableName} (${columnNames}, created_at) VALUES (${placeholders}, NOW())`;

    for (const row of gstOnlyRows) {
      const values = sanitizedCols.map((sanitized) => {
        let value = row?.[sanitized] ?? null;
        if (value && typeof value === 'string' && /^\d{4}-\d{2}-\d{2}T/.test(value)) {
          value = value.split('T')[0];
        }
        return value;
      });
      await pool.query(insertSql, values);
    }

    return res.json({
      success: true,
      logId,
      tableName,
      recordsCount: gstOnlyRows.length,
      message: 'GST-only records saved successfully'
    });
  } catch (error) {
    console.error('Error saving GST-only records:', error);
    return res.status(500).json({ error: error.message });
  }
});

// Save Tally-only records to database
app.post('/api/save-tally-only', async (req, res) => {
  try {
    const { logId, tallyColumns, tallyOnlyRows } = req.body;

    if (!logId || !tallyColumns || !tallyOnlyRows || tallyOnlyRows.length === 0) {
      return res.status(400).json({ error: 'Missing required parameters' });
    }

    const parsedLogId = Number(logId);
    if (!Number.isInteger(parsedLogId) || parsedLogId <= 0) {
      return res.status(400).json({ error: 'Invalid logId' });
    }

    const tableName = `tally_only_${parsedLogId}`;

    const sanitizeColumnName = (name) => {
      if (!name) return 'col';
      let sanitized = String(name).toLowerCase().replace(/[^a-z0-9_]/g, '_').replace(/_+/g, '_').replace(/^_|_$/g, '');
      if (!sanitized) sanitized = 'col';
      if (/^\d/.test(sanitized)) sanitized = 'col_' + sanitized;
      return sanitized;
    };

    const originalColumns = Array.isArray(tallyColumns) ? tallyColumns.filter((c) => c) : [];
    if (originalColumns.length === 0) {
      return res.status(400).json({ error: 'No Tally columns provided' });
    }

    const seen = new Map();
    const sanitizedCols = originalColumns.map((orig) => {
      const base = sanitizeColumnName(orig);
      const count = (seen.get(base) || 0) + 1;
      seen.set(base, count);
      return count === 1 ? base : `${base}_${count}`;
    });

    const columnsSQL = sanitizedCols
      .map(col => `"${col}" TEXT`)
      .join(', ');

    await pool.query(`
      CREATE TABLE IF NOT EXISTS ${tableName} (
        id SERIAL PRIMARY KEY,
        ${columnsSQL},
        created_at TIMESTAMP DEFAULT NOW()
      )
    `);

    const placeholders = sanitizedCols.map((_, i) => `$${i + 1}`).join(', ');
    const columnNames = sanitizedCols.map((c) => `"${c}"`).join(', ');
    const insertSql = `INSERT INTO ${tableName} (${columnNames}, created_at) VALUES (${placeholders}, NOW())`;

    for (const row of tallyOnlyRows) {
      const values = sanitizedCols.map((sanitized) => {
        let value = row?.[sanitized] ?? null;
        if (value && typeof value === 'string' && /^\d{4}-\d{2}-\d{2}T/.test(value)) {
          value = value.split('T')[0];
        }
        return value;
      });
      await pool.query(insertSql, values);
    }

    return res.json({
      success: true,
      logId,
      tableName,
      recordsCount: tallyOnlyRows.length,
      message: 'Tally-only records saved successfully'
    });
  } catch (error) {
    console.error('Error saving Tally-only records:', error);
    return res.status(500).json({ error: error.message });
  }
});

// Save Partial Minor matches to database (stores full GST + Tally rows as JSONB)
app.post('/api/save-partial-minor', async (req, res) => {
  try {
    const { logId, partialMatches } = req.body;

    if (!logId || !partialMatches || partialMatches.length === 0) {
      return res.status(400).json({ error: 'Missing required parameters' });
    }

    const parsedLogId = Number(logId);
    if (!Number.isInteger(parsedLogId) || parsedLogId <= 0) {
      return res.status(400).json({ error: 'Invalid logId' });
    }

    const tableName = `partial_minor_${parsedLogId}`;

    await pool.query(`
      CREATE TABLE IF NOT EXISTS ${tableName} (
        id SERIAL PRIMARY KEY,
        gst JSONB,
        tally JSONB,
        discrepancies INTEGER,
        max_discrepancy NUMERIC,
        created_at TIMESTAMP DEFAULT NOW()
      )
    `);

    const insertSql = `INSERT INTO ${tableName} (gst, tally, discrepancies, max_discrepancy, created_at) VALUES ($1, $2, $3, $4, NOW())`;

    for (const match of partialMatches) {
      const gst = match?.gst ?? null;
      const tally = match?.tally ?? null;
      const discrepancies = Number(match?.discrepancies ?? 0);
      const maxDiscrepancy = Number(match?.maxDiscrepancy ?? match?.max_discrepancy ?? 0);
      await pool.query(insertSql, [gst, tally, Number.isFinite(discrepancies) ? discrepancies : 0, Number.isFinite(maxDiscrepancy) ? maxDiscrepancy : 0]);
    }

    return res.json({
      success: true,
      logId,
      tableName,
      recordsCount: partialMatches.length,
      message: 'Partial minor matches saved successfully'
    });
  } catch (error) {
    console.error('Error saving partial minor matches:', error);
    return res.status(500).json({ error: error.message });
  }
});

// Save Partial Major matches to database (stores full GST + Tally rows as JSONB)
app.post('/api/save-partial-major', async (req, res) => {
  try {
    const { logId, partialMatches } = req.body;

    if (!logId || !partialMatches || partialMatches.length === 0) {
      return res.status(400).json({ error: 'Missing required parameters' });
    }

    const parsedLogId = Number(logId);
    if (!Number.isInteger(parsedLogId) || parsedLogId <= 0) {
      return res.status(400).json({ error: 'Invalid logId' });
    }

    const tableName = `partial_major_${parsedLogId}`;

    await pool.query(`
      CREATE TABLE IF NOT EXISTS ${tableName} (
        id SERIAL PRIMARY KEY,
        gst JSONB,
        tally JSONB,
        discrepancies INTEGER,
        max_discrepancy NUMERIC,
        created_at TIMESTAMP DEFAULT NOW()
      )
    `);

    const insertSql = `INSERT INTO ${tableName} (gst, tally, discrepancies, max_discrepancy, created_at) VALUES ($1, $2, $3, $4, NOW())`;

    for (const match of partialMatches) {
      const gst = match?.gst ?? null;
      const tally = match?.tally ?? null;
      const discrepancies = Number(match?.discrepancies ?? 0);
      const maxDiscrepancy = Number(match?.maxDiscrepancy ?? match?.max_discrepancy ?? 0);
      await pool.query(insertSql, [gst, tally, Number.isFinite(discrepancies) ? discrepancies : 0, Number.isFinite(maxDiscrepancy) ? maxDiscrepancy : 0]);
    }

    return res.json({
      success: true,
      logId,
      tableName,
      recordsCount: partialMatches.length,
      message: 'Partial major matches saved successfully'
    });
  } catch (error) {
    console.error('Error saving partial major matches:', error);
    return res.status(500).json({ error: error.message });
  }
});

// Save mapping endpoint - stores mapped data with actual column names and date detection
app.post('/api/save-mapping', async (req, res) => {
  try {
    const { uploadId, gstColumns, tallyColumns, gstHeaderRow, tallyHeaderRow } = req.body;

    if (!uploadId || !gstColumns || !tallyColumns) {
      return res.status(400).json({ error: 'Missing required parameters' });
    }

    // Get upload data
    const uploadResult = await pool.query('SELECT gst_data, tally_data FROM uploads WHERE id = $1', [uploadId]);
    if (uploadResult.rows.length === 0) {
      return res.status(404).json({ error: 'Upload not found' });
    }

    const { gst_data, tally_data } = uploadResult.rows[0];
    const gstData = typeof gst_data === 'string' ? JSON.parse(gst_data) : gst_data;
    const tallyData = typeof tally_data === 'string' ? JSON.parse(tally_data) : tally_data;

    // Filter out empty mappings
    const validMappings = gstColumns
      .map((gstCol, idx) => ({
        gstCol,
        tallyCol: tallyColumns[idx]
      }))
      .filter(m => m.gstCol && m.tallyCol);

    if (validMappings.length === 0) {
      return res.status(400).json({ error: 'No valid column mappings found' });
    }

    console.log('=== SAVE MAPPING DEBUG ===');
    console.log('GST Columns:', JSON.stringify(gstColumns));
    console.log('Tally Columns:', JSON.stringify(tallyColumns));
    console.log('Valid Mappings:');
    validMappings.forEach((m, idx) => {
      console.log(`  [${idx}] GST: "${m.gstCol}" -> Tally: "${m.tallyCol}"`);
    });
    
    // Show which columns will be date columns
    console.log('\n=== DATE COLUMN DETECTION ===');
    validMappings.forEach((m, idx) => {
      const gstIsInvoiceDate = m.gstCol.toLowerCase().trim() === 'invoice date';
      const tallyIsInvoiceDate = m.tallyCol.toLowerCase().trim() === 'invoice date';
      if (gstIsInvoiceDate || tallyIsInvoiceDate) {
        console.log(`  [${idx}] POTENTIAL DATE: GST="${m.gstCol}" (${gstIsInvoiceDate ? 'YES' : 'NO'}) -> Tally="${m.tallyCol}" (${tallyIsInvoiceDate ? 'YES' : 'NO'})`);
      }
    });

    // Helper functions
    const isValidDate = (year, month, day) => {
      year = parseInt(year);
      month = parseInt(month);
      day = parseInt(day);
      
      if (month < 1 || month > 12) return false;
      if (day < 1 || day > 31) return false;
      if (year < 1900 || year > 2100) return false;
      
      return true;
    };

    const isDateValue = (value) => {
      if (!value) return false;
      const str = String(value).trim();
      
      // Check if it's an Excel serial number (5-digit number in date range)
      const numVal = parseFloat(str);
      if (!isNaN(numVal) && numVal > 30000 && numVal < 50000) {
        return true; // Excel serial numbers are dates
      }
      
      // Must match date pattern (DD/MM/YYYY, DD-MM-YYYY, YYYY/MM/DD, etc.)
      if (!/^\d{1,4}[-/]\d{1,2}[-/]\d{1,4}$/.test(str)) return false;
      
      const parts = str.split(/[-/]/);
      if (parts.length !== 3) return false;
      
      let year, month, day;
      if (parts[0].length === 4) {
        [year, month, day] = parts;
      } else if (parts[2].length === 4) {
        [day, month, year] = parts;
      } else {
        return false;
      }
      
      // Validate date components
      return isValidDate(year, month, day);
    };

    const excelSerialToDate = (serial) => {
      // Handle both number and string inputs
      let numSerial = typeof serial === 'string' ? parseFloat(serial) : serial;
      
      if (isNaN(numSerial) || numSerial < 0) return null;
      
      // Excel epoch: January 1, 1900
      const excelEpoch = new Date(1900, 0, 1);
      const millisecondsPerDay = 24 * 60 * 60 * 1000;
      
      // Account for Excel's leap year bug (Feb 29, 1900 doesn't exist but Excel thinks it does)
      let adjustedSerial = numSerial;
      if (numSerial > 59) {
        adjustedSerial = numSerial - 1;
      }
      
      const date = new Date(excelEpoch.getTime() + (adjustedSerial - 1) * millisecondsPerDay);
      
      const day = String(date.getDate()).padStart(2, '0');
      const month = String(date.getMonth() + 1).padStart(2, '0');
      const year = date.getFullYear();
      
      return `${year}-${month}-${day}`;
    };

    const convertToStandardDate = (value) => {
      if (!value) return null;
      
      const str = String(value).trim();
      
      // Check if it's an Excel serial number (typically 5-digit numbers for dates)
      // Handle both number and string representations
      const numVal = parseFloat(str);
      if (!isNaN(numVal) && numVal > 30000 && numVal < 50000) {
        const dateStr = excelSerialToDate(numVal);
        if (dateStr) {
          console.log(`Converted Excel serial ${str} to date ${dateStr}`);
          return dateStr;
        }
      }
      
      // Try to parse as date string (DD/MM/YYYY, DD-MM-YYYY, YYYY/MM/DD, YYYY-MM-DD, etc.)
      const parts = str.split(/[-/]/);
      if (parts.length !== 3) return null;
      
      let year, month, day;
      if (parts[0].length === 4) {
        [year, month, day] = parts;
      } else if (parts[2].length === 4) {
        [day, month, year] = parts;
      } else {
        return null;
      }
      
      // Validate before converting
      if (!isValidDate(year, month, day)) return null;
      
      month = String(month).padStart(2, '0');
      day = String(day).padStart(2, '0');
      return `${year}-${month}-${day}`;
    };

    const sanitizeColumnName = (name) => {
      if (!name) return 'col_' + Date.now();
      let sanitized = name.toLowerCase().replace(/[^a-z0-9_]/g, '_').replace(/_+/g, '_').replace(/^_|_$/g, '');
      if (!sanitized) return 'col_' + Date.now();
      // Prepend 'col_' if starts with a number
      if (/^\d/.test(sanitized)) {
        sanitized = 'col_' + sanitized;
      }
      return sanitized;
    };

    // Skip rows before header row
    const gstDataFiltered = gstData.slice(gstHeaderRow - 1);
    const tallyDataFiltered = tallyData.slice(tallyHeaderRow - 1);

    // Detect date columns - ONLY "Invoice Date" column
    const gstDateColumns = new Set();
    const tallyDateColumns = new Set();
    
    validMappings.forEach(m => {
      console.log(`Checking mapping: GST="${m.gstCol}" -> Tally="${m.tallyCol}"`);
      
      // Only check for exact column name "Invoice Date" (case-insensitive)
      const gstIsInvoiceDate = m.gstCol.toLowerCase().trim() === 'invoice date';
      const tallyIsInvoiceDate = m.tallyCol.toLowerCase().trim() === 'invoice date';

      console.log(`  GST is Invoice Date: ${gstIsInvoiceDate}, Tally is Invoice Date: ${tallyIsInvoiceDate}`);

      // Check GST column - only if it's "Invoice Date"
      if (gstIsInvoiceDate) {
        let gstAllDatesOrEmpty = true;
        let gstHasAtLeastOneDate = false;
        for (let i = 0; i < Math.min(10, gstDataFiltered.length); i++) {
          if (gstDataFiltered[i]) {
            const val = gstDataFiltered[i][m.gstCol];
            if (val && String(val).trim()) {
              if (isDateValue(val)) {
                gstHasAtLeastOneDate = true;
              } else {
                gstAllDatesOrEmpty = false;
                break;
              }
            }
          }
        }
        if (gstAllDatesOrEmpty && gstHasAtLeastOneDate) {
          gstDateColumns.add(m.gstCol);
          console.log('✓ GST Invoice Date column detected and will be converted');
        }
      }

      // Check Tally column - only if it's "Invoice Date"
      if (tallyIsInvoiceDate) {
        let tallyAllDatesOrEmpty = true;
        let tallyHasAtLeastOneDate = false;
        for (let i = 0; i < Math.min(10, tallyDataFiltered.length); i++) {
          if (tallyDataFiltered[i]) {
            const val = tallyDataFiltered[i][m.tallyCol];
            if (val && String(val).trim()) {
              if (isDateValue(val)) {
                tallyHasAtLeastOneDate = true;
              } else {
                tallyAllDatesOrEmpty = false;
                break;
              }
            }
          }
        }
        if (tallyAllDatesOrEmpty && tallyHasAtLeastOneDate) {
          tallyDateColumns.add(m.tallyCol);
          console.log('✓ Tally Invoice Date column detected and will be converted');
        }
      }
    });
    
    console.log('Final date columns - GST:', Array.from(gstDateColumns));
    console.log('Final date columns - Tally:', Array.from(tallyDateColumns));

    // Create dynamic table names
    const logId = Date.now();
    const gstTableName = `gst_mapped_log_${logId}`;
    const tallyTableName = `tally_mapped_log_${logId}`;

    try {
      // Create GST table with actual column names
      const gstColumns_sql = validMappings
        .map(m => {
          const colName = sanitizeColumnName(m.gstCol);
          const isDate = gstDateColumns.has(m.gstCol);
          return `${colName} ${isDate ? 'DATE' : 'TEXT'}`;
        })
        .join(', ');

      console.log('Creating GST table:', gstTableName);
      console.log('GST columns SQL:', gstColumns_sql);

      await pool.query(`
        CREATE TABLE IF NOT EXISTS ${gstTableName} (
          id SERIAL PRIMARY KEY,
          ${gstColumns_sql},
          created_at TIMESTAMP DEFAULT NOW()
        )
      `);
      console.log('GST table created successfully');
    } catch (err) {
      console.error('Error creating GST table:', err.message);
      throw err;
    }

    try {
      // Create Tally table with actual column names
      const tallyColumns_sql = validMappings
        .map(m => {
          const colName = sanitizeColumnName(m.tallyCol);
          const isDate = tallyDateColumns.has(m.tallyCol);
          return `${colName} ${isDate ? 'DATE' : 'TEXT'}`;
        })
        .join(', ');

      console.log('Creating Tally table:', tallyTableName);
      console.log('Tally columns SQL:', tallyColumns_sql);

      await pool.query(`
        CREATE TABLE IF NOT EXISTS ${tallyTableName} (
          id SERIAL PRIMARY KEY,
          ${tallyColumns_sql},
          created_at TIMESTAMP DEFAULT NOW()
        )
      `);
      console.log('Tally table created successfully');
    } catch (err) {
      console.error('Error creating Tally table:', err.message);
      throw err;
    }

    // Insert GST data
    try {
      console.log('Inserting GST data, rows:', gstDataFiltered.length);
      for (const row of gstDataFiltered) {
        const values = validMappings.map(m => {
          const val = row[m.gstCol] || null;
          if (gstDateColumns.has(m.gstCol)) {
            return convertToStandardDate(val);
          }
          return normalizeValueForDB(val);
        });
        const placeholders = validMappings.map((_, i) => `$${i + 1}`).join(', ');
        const columnNames = validMappings.map(m => sanitizeColumnName(m.gstCol)).join(', ');

        await pool.query(
          `INSERT INTO ${gstTableName} (${columnNames}, created_at) VALUES (${placeholders}, NOW())`,
          values
        );
      }
      console.log('GST data inserted successfully');
    } catch (err) {
      console.error('Error inserting GST data:', err.message);
      throw err;
    }

    // Insert Tally data
    try {
      console.log('Inserting Tally data, rows:', tallyDataFiltered.length);
      for (const row of tallyDataFiltered) {
        const values = validMappings.map(m => {
          const val = row[m.tallyCol] || null;
          if (tallyDateColumns.has(m.tallyCol)) {
            return convertToStandardDate(val);
          }
          return normalizeValueForDB(val);
        });
        const placeholders = validMappings.map((_, i) => `$${i + 1}`).join(', ');
        const columnNames = validMappings.map(m => sanitizeColumnName(m.tallyCol)).join(', ');

        await pool.query(
          `INSERT INTO ${tallyTableName} (${columnNames}, created_at) VALUES (${placeholders}, NOW())`,
          values
        );
      }
      console.log('Tally data inserted successfully');
    } catch (err) {
      console.error('Error inserting Tally data:', err.message);
      throw err;
    }

    // Store mapping log
    const logResult = await pool.query(
      `INSERT INTO mapping_logs (upload_id, gst_columns, tally_columns, gst_header_row, tally_header_row, gst_table_name, tally_table_name, created_at) 
       VALUES ($1, $2, $3, $4, $5, $6, $7, NOW()) RETURNING id`,
      [uploadId, gstColumns, tallyColumns, gstHeaderRow, tallyHeaderRow, gstTableName, tallyTableName]
    );

    const savedLogId = logResult.rows[0].id;
    console.log(`Mapping saved successfully. Log ID: ${savedLogId}`);

    return res.status(200).json({
      success: true,
      logId: savedLogId,
      gstTableName,
      tallyTableName,
      message: 'Mapping saved successfully'
    });
  } catch (error) {
    console.error('Save mapping error:', error);
    return res.status(500).json({ error: error.message });
  }
});

// Error handling middleware
app.use((err, req, res, next) => {
  console.error(err);
  res.status(500).json({ error: 'Internal server error' });
});

// Start server
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
