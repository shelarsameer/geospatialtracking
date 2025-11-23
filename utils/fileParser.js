import XLSX from 'xlsx';
import fs from 'fs';
import { parse } from 'csv-parse/sync';

// Convert Excel serial number to date string (DD-MM-YYYY)
function excelSerialToDate(serial) {
  if (typeof serial !== 'number' || serial < 0) return null;
  
  // Excel epoch: January 1, 1900
  const excelEpoch = new Date(1900, 0, 1);
  const millisecondsPerDay = 24 * 60 * 60 * 1000;
  
  // Account for Excel's leap year bug (Feb 29, 1900 doesn't exist but Excel thinks it does)
  let adjustedSerial = serial;
  if (serial > 59) {
    adjustedSerial = serial - 1;
  }
  
  const date = new Date(excelEpoch.getTime() + (adjustedSerial - 1) * millisecondsPerDay);
  
  const day = String(date.getDate()).padStart(2, '0');
  const month = String(date.getMonth() + 1).padStart(2, '0');
  const year = date.getFullYear();
  
  return `${day}-${month}-${year}`;
}

// Normalize values: replace '-' with '0'
// DO NOT normalize date separators or convert Excel serial numbers - that will be done in backend for specific date columns only
function normalizeValue(value) {
  if (value === null || value === undefined || value === '') {
    return 0;
  }

  // Convert to string for processing
  let strValue = String(value).trim();

  // Replace standalone '-' with '0'
  if (strValue === '-') {
    return 0;
  }

  // DO NOT normalize date separators here - this causes invoice numbers like "2025-26-33" to be converted
  // The backend will handle date conversion for specific date columns only

  return strValue;
}

export function parseExcel(filePath) {
  try {
    const workbook = XLSX.readFile(filePath);
    const sheetName = workbook.SheetNames[0];
    const worksheet = workbook.Sheets[sheetName];
    
    // Get raw data with all columns
    const data = XLSX.utils.sheet_to_json(worksheet, { defval: 0 });
    
    // Get all unique column names from all rows
    const allColumns = new Set();
    data.forEach(row => {
      Object.keys(row).forEach(col => allColumns.add(col));
    });
    
    // Normalize all rows to have all columns with normalized values
    const normalizedData = data.map(row => {
      const normalizedRow = {};
      allColumns.forEach(col => {
        const value = row[col] !== undefined ? row[col] : 0;
        normalizedRow[col] = normalizeValue(value);
      });
      return normalizedRow;
    });
    
    // Clean up file
    fs.unlinkSync(filePath);
    
    return normalizedData;
  } catch (error) {
    throw new Error(`Failed to parse Excel file: ${error.message}`);
  }
}

export function parseCSV(filePath) {
  try {
    const fileContent = fs.readFileSync(filePath, 'utf-8');
    const data = parse(fileContent, {
      columns: true,
      skip_empty_lines: true,
      trim: true
    });
    
    // Normalize all values in CSV data
    const normalizedData = data.map(row => {
      const normalizedRow = {};
      Object.keys(row).forEach(col => {
        normalizedRow[col] = normalizeValue(row[col]);
      });
      return normalizedRow;
    });
    
    // Clean up file
    fs.unlinkSync(filePath);
    
    return normalizedData;
  } catch (error) {
    throw new Error(`Failed to parse CSV file: ${error.message}`);
  }
}
