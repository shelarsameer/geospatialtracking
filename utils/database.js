export async function initializeDatabase(pool) {
  try {
    // Create uploads table
    await pool.query(`
      CREATE TABLE IF NOT EXISTS uploads (
        id SERIAL PRIMARY KEY,
        gst_data JSONB NOT NULL,
        tally_data JSONB NOT NULL,
        created_at TIMESTAMP DEFAULT NOW()
      )
    `);

    // Create reconciliation_results table
    await pool.query(`
      CREATE TABLE IF NOT EXISTS reconciliation_results (
        id SERIAL PRIMARY KEY,
        upload_id INTEGER REFERENCES uploads(id),
        exact_matches JSONB DEFAULT '[]',
        partial_matches JSONB DEFAULT '[]',
        tally_mismatches JSONB DEFAULT '[]',
        gst_mismatches JSONB DEFAULT '[]',
        gst_header_row INTEGER DEFAULT 1,
        tally_header_row INTEGER DEFAULT 1,
        created_at TIMESTAMP DEFAULT NOW()
      )
    `);

    // Create column_mappings table to store mapping metadata
    await pool.query(`
      CREATE TABLE IF NOT EXISTS column_mappings (
        id SERIAL PRIMARY KEY,
        reconciliation_id INTEGER REFERENCES reconciliation_results(id),
        gst_columns TEXT[] NOT NULL,
        tally_columns TEXT[] NOT NULL,
        created_at TIMESTAMP DEFAULT NOW()
      )
    `);

    // Create mapping_logs table to store saved mappings with their data
    await pool.query(`
      CREATE TABLE IF NOT EXISTS mapping_logs (
        id SERIAL PRIMARY KEY,
        upload_id INTEGER REFERENCES uploads(id),
        gst_columns TEXT[] NOT NULL,
        tally_columns TEXT[] NOT NULL,
        gst_header_row INTEGER DEFAULT 1,
        tally_header_row INTEGER DEFAULT 1,
        gst_table_name TEXT NOT NULL,
        tally_table_name TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT NOW()
      )
    `);

    console.log('Database initialized successfully');
  } catch (error) {
    console.error('Database initialization error:', error);
    throw error;
  }
}
