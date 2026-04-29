// ── MODIS MOD11A2 - Quick Visualization Check ──────────────────────────────
// Run this first to verify data loads correctly over your 5 cities

var dataset = ee.ImageCollection('MODIS/061/MOD11A2')
  .filter(ee.Filter.date('2023-04-01', '2023-05-01'))
  .select('LST_Day_1km');

// Convert raw DN to Celsius
var toLST_Celsius = function(image) {
  return image.multiply(0.02).subtract(273.15)
    .copyProperties(image, ['system:time_start']);
};
var lstCelsius = dataset.map(toLST_Celsius);
var lstMean = lstCelsius.mean();

// City center coordinates for centering the map
var cities = {
  Delhi:     [77.209, 28.614],
  Mumbai:    [72.878, 19.076],
  Bangalore: [77.594, 12.972],
  Chennai:   [80.270, 13.083],
  Hyderabad: [78.487, 17.385]
};

// Visualization palette (cool blue → hot red)
var visParams = {
  min: 20, max: 55,
  palette: ['040274','0602ff','235cb1','30c8e2','3be285',
            '86e26f','fff705','ffb613','ff6e08','ff0000','911003']
};

Map.setCenter(78.96, 20.59, 5); // India-wide view
Map.addLayer(lstMean, visParams, 'Mean Daytime LST Apr 2023 (°C)');
print('Image count:', dataset.size());
print('First image date:', dataset.first().date());
