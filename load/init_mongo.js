// MongoDB initialization script for economic_indicators collection
// Run: mongosh < init_mongo.js  OR  docker exec mongo mongosh < init_mongo.js

db = db.getSiblingDB('diploma');

// Create collection if not exists
db.createCollection('economic_indicators');

// Unique compound index: one record per country + year + indicator
db.economic_indicators.createIndex(
    { country_code: 1, year: 1, indicator_type: 1 },
    { unique: true, name: 'idx_country_year_indicator_unique' }
);

// Time-series queries per indicator
db.economic_indicators.createIndex(
    { indicator_type: 1, year: 1 },
    { name: 'idx_indicator_year' }
);

// Country-level queries
db.economic_indicators.createIndex(
    { country_code: 1 },
    { name: 'idx_country' }
);

print('Indexes created on diploma.economic_indicators:');
db.economic_indicators.getIndexes().forEach(function(idx) {
    print('  - ' + idx.name + ': ' + JSON.stringify(idx.key));
});
