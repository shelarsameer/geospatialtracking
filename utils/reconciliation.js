export function reconcileData(gstData, tallyData, gstColumns, tallyColumns) {
  const exactMatches = [];
  const partialMatches = [];
  const tallyMismatches = [];
  const gstMismatches = [];

  const matchedTallyIds = new Set();
  const matchedGstIds = new Set();

  // Create lookup maps for faster searching
  const tallyMap = new Map();
  tallyData.forEach((record, index) => {
    const key = createKey(record, tallyColumns);
    if (!tallyMap.has(key)) {
      tallyMap.set(key, []);
    }
    tallyMap.get(key).push({ ...record, _index: index });
  });

  // Match GST records with Tally records
  gstData.forEach((gstRecord, gstIndex) => {
    const gstKey = createKey(gstRecord, gstColumns);
    
    // Check for exact match
    if (tallyMap.has(gstKey)) {
      const tallyMatches = tallyMap.get(gstKey);
      tallyMatches.forEach(tallyRecord => {
        if (!matchedTallyIds.has(tallyRecord._index)) {
          exactMatches.push({
            gstRecord,
            tallyRecord: { ...tallyRecord },
            matchType: 'exact',
            discrepancies: []
          });
          matchedTallyIds.add(tallyRecord._index);
          matchedGstIds.add(gstIndex);
        }
      });
    }
  });

  // Check for partial matches (if not already matched exactly)
  gstData.forEach((gstRecord, gstIndex) => {
    if (matchedGstIds.has(gstIndex)) return;

    tallyData.forEach((tallyRecord, tallyIndex) => {
      if (matchedTallyIds.has(tallyIndex)) return;

      const discrepancies = findDiscrepancies(gstRecord, tallyRecord, gstColumns, tallyColumns);
      
      // Partial match if at least one key field matches
      if (discrepancies.length > 0 && discrepancies.length < gstColumns.length) {
        partialMatches.push({
          gstRecord,
          tallyRecord,
          matchType: 'partial',
          discrepancies
        });
        matchedTallyIds.add(tallyIndex);
        matchedGstIds.add(gstIndex);
      }
    });
  });

  // Find unmatched records
  gstData.forEach((gstRecord, gstIndex) => {
    if (!matchedGstIds.has(gstIndex)) {
      gstMismatches.push({
        record: gstRecord,
        source: 'GST',
        type: 'missing_in_tally'
      });
    }
  });

  tallyData.forEach((tallyRecord, tallyIndex) => {
    if (!matchedTallyIds.has(tallyIndex)) {
      tallyMismatches.push({
        record: tallyRecord,
        source: 'Tally',
        type: 'missing_in_gst'
      });
    }
  });

  return {
    exactMatches,
    partialMatches,
    tallyMismatches,
    gstMismatches
  };
}

function createKey(record, columns) {
  return columns
    .map(col => String(record[col] || '').trim().toLowerCase())
    .join('|');
}

function findDiscrepancies(gstRecord, tallyRecord, gstColumns, tallyColumns) {
  const discrepancies = [];
  
  for (let i = 0; i < Math.min(gstColumns.length, tallyColumns.length); i++) {
    const gstCol = gstColumns[i];
    const tallyCol = tallyColumns[i];
    
    const gstValue = String(gstRecord[gstCol] || '').trim().toLowerCase();
    const tallyValue = String(tallyRecord[tallyCol] || '').trim().toLowerCase();
    
    if (gstValue !== tallyValue) {
      discrepancies.push({
        field: gstCol,
        gstValue: gstRecord[gstCol],
        tallyValue: tallyRecord[tallyCol]
      });
    }
  }
  
  return discrepancies;
}
