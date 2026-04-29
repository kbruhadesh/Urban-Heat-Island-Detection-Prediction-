// ── Quick test: Delhi 2023 only ──────────────────────────────────────────────

var Delhi = ee.Geometry.Rectangle([76.84, 28.40, 77.35, 28.88]);
var ruralDelhi = Delhi.buffer(20000).difference(Delhi);

var applyQAMask = function(image) {
  return image.updateMask(image.select('QC_Day').bitwiseAnd(3).eq(0));
};

var modis2023 = ee.ImageCollection('MODIS/061/MOD11A2')
  .filter(ee.Filter.date('2023-01-01', '2023-12-31'))
  .map(applyQAMask);

var timeSeries = modis2023.map(function(image) {
  var lst = image.select('LST_Day_1km').multiply(0.02).subtract(273.15);
  var urban = lst.reduceRegion({reducer: ee.Reducer.mean(), geometry: Delhi,     scale: 1000, maxPixels: 1e9});
  var rural  = lst.reduceRegion({reducer: ee.Reducer.mean(), geometry: ruralDelhi, scale: 1000, maxPixels: 1e9});
  return ee.Feature(null, {
    date:        image.date().format('YYYY-MM-dd'),
    LST_Urban:   urban.get('LST_Day_1km'),
    LST_Rural:   rural.get('LST_Day_1km'),
    UHI_Index:   ee.Number(urban.get('LST_Day_1km'))
                   .subtract(ee.Number(rural.get('LST_Day_1km')))
  });
});

print('Delhi 2023 sample (first 5):', timeSeries.limit(5));

Export.table.toDrive({
  collection: ee.FeatureCollection(timeSeries),
  description: 'MODIS_Delhi_2023_TEST',
  folder: 'UHI_Project/MODIS',
  fileFormat: 'CSV'
});