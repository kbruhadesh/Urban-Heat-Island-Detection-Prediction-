// ── MODIS MOD11A2 - Full Export FIXED + MORE CITIES ───────────────────────

// ── 1. City boundaries ────────────────────────────────────────────────────
var cities = {
  Delhi:     ee.Geometry.Rectangle([76.84, 28.40, 77.35, 28.88]),
  Mumbai:    ee.Geometry.Rectangle([72.77, 18.89, 73.00, 19.27]),
  Bangalore: ee.Geometry.Rectangle([77.46, 12.83, 77.74, 13.14]),
  Chennai:   ee.Geometry.Rectangle([80.15, 12.92, 80.33, 13.23]),
  Hyderabad: ee.Geometry.Rectangle([78.31, 17.27, 78.62, 17.54]),

  Kochi:     ee.Geometry.Rectangle([76.20, 9.85, 76.40, 10.10]),
  Pune:      ee.Geometry.Rectangle([73.75, 18.45, 73.98, 18.70]),
  Ahmedabad: ee.Geometry.Rectangle([72.45, 22.90, 72.75, 23.15]),
  Kolkata:   ee.Geometry.Rectangle([88.20, 22.45, 88.50, 22.75]),
  Jaipur:    ee.Geometry.Rectangle([75.65, 26.75, 75.95, 27.05]),
  Surat:     ee.Geometry.Rectangle([72.70, 21.10, 72.95, 21.30])
};

// ── 2. Preprocess ─────────────────────────────────────────────────────────
var preprocessImage = function(image) {
  var qa = image.select('QC_Day');
  var goodPixels = qa.bitwiseAnd(3).eq(0);

  var lstDay = image.select('LST_Day_1km').updateMask(goodPixels);
  var lstNight = image.select('LST_Night_1km').updateMask(goodPixels);

  var lstDayC   = lstDay.multiply(0.02).subtract(273.15).rename('LST_Day_C');
  var lstNightC = lstNight.multiply(0.02).subtract(273.15).rename('LST_Night_C');

  return lstDayC.addBands(lstNightC)
    .copyProperties(image, ['system:time_start', 'system:index']);
};

// ── 3. Load dataset ────────────────────────────────────────────────────────
var modis = ee.ImageCollection('MODIS/061/MOD11A2')
  .filter(ee.Filter.date('2000-01-01', '2024-12-31'))
  .map(preprocessImage);

// ── 4. Monthly composites ──────────────────────────────────────────────────
var years  = ee.List.sequence(2000, 2024);
var months = ee.List.sequence(1, 12);

var monthlyCollection = ee.ImageCollection.fromImages(
  years.map(function(y) {
    return months.map(function(m) {

      var filtered = modis
        .filter(ee.Filter.calendarRange(y, y, 'year'))
        .filter(ee.Filter.calendarRange(m, m, 'month'));

      var monthly = ee.Algorithms.If(
        filtered.size().gt(0),
        filtered.mean(),
        ee.Image.constant([-9999, -9999])
          .rename(['LST_Day_C', 'LST_Night_C'])
      );

      return ee.Image(monthly)
        .set('year', y)
        .set('month', m);
    });
  }).flatten()
);

// ── 5. Export per city ─────────────────────────────────────────────────────
var cityNames = Object.keys(cities);

cityNames.forEach(function(cityName) {
  var urbanGeom = cities[cityName];
  var ruralGeom = urbanGeom.buffer(20000).difference(urbanGeom);

  var timeSeries = monthlyCollection.map(function(image) {
    var dayImg   = image.select('LST_Day_C').unmask(-9999);
    var nightImg = image.select('LST_Night_C').unmask(-9999);

    var urbanDay = dayImg.reduceRegion({
      reducer: ee.Reducer.mean(),
      geometry: urbanGeom,
      scale: 1000,
      maxPixels: 1e9
    });

    var ruralDay = dayImg.reduceRegion({
      reducer: ee.Reducer.mean(),
      geometry: ruralGeom,
      scale: 1000,
      maxPixels: 1e9
    });

    var urbanNight = nightImg.reduceRegion({
      reducer: ee.Reducer.mean(),
      geometry: urbanGeom,
      scale: 1000,
      maxPixels: 1e9
    });

    var urbanVal = ee.Number(urbanDay.get('LST_Day_C'));
    var ruralVal = ee.Number(ruralDay.get('LST_Day_C'));

    return ee.Feature(null, {
      city: cityName,
      year: image.get('year'),
      month: image.get('month'),
      LST_Day_Urban: urbanVal,
      LST_Day_Rural: ruralVal,
      LST_Night_Urban: ee.Number(urbanNight.get('LST_Night_C')),
      UHI_Index: urbanVal.subtract(ruralVal)
    });
  });

  Export.table.toDrive({
    collection: ee.FeatureCollection(timeSeries),
    description: 'MODIS_LST_' + cityName + '_2000_2024',
    folder: 'UHI_Project',
    fileFormat: 'CSV'
  });
});

print('All export tasks created.');