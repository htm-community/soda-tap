$(function() {

    var DEFAULT_DATA_LIMIT = 100;
    var SCALAR_DATA_LIMIT = 5000;
    var GEO_DATA_LIMIT = 1000;

    function graphData(id, data, graphLabels, temporalField, typeIndex) {
        var graphInput = [];
        _.each(data, function(point) {
            var row = [];
            row.push(new Date(point[temporalField]));
            for (var i = 1; i < graphLabels.length; i++) {
                row.push(new Number(point[graphLabels[i]]))
            }
            graphInput.push(row);
        });
        new Dygraph(
            document.getElementById(id + '-viz'),
            graphInput,
            {
                labels: graphLabels,
                width: $('.panel-body').width()
            }
        );
    }

    function renderMap(id, data, temporalField) {
        var vizContainer = $('#' + id + '-viz');
        vizContainer.html('<div class="map" id="' + id + '-map"></div>');
        var markers = [];
        var map = new google.maps.Map(document.getElementById(id + '-map'), {
            center: {lat: -34.397, lng: 150.644},
            zoom: 8
        });
        var lats = []
        var lons = []

        _.each(data, function(point) {
            var lat, lon;
            if (point.latitude && point.longitude) {
                lat = point.latitude;
                lon = point.longitude;
            } else {
                _.each(point, function(val, key) {
                    if (val.type && val.type == 'Point') {
                        lat = val.coordinates[1];
                        lon = val.coordinates[0];
                    } else if (val.latitude) {
                        lat = val.latitude;
                        lon = val.longitude;
                    }
                });
            }
            if (lat && lon) {
                var myLatLng = {
                    lat: parseFloat(lat),
                    lng: parseFloat(lon)
                };
                markers.push(new google.maps.Marker({
                    position: myLatLng,
                    map: map,
                    title: point[temporalField]
                }));
                lats.push(lat);
                lons.push(lon);
            }
        });

        map.setCenter({
            lat: _.sum(lats) / lats.length,
            lng: _.sum(lons) / lons.length
        });
    }

    function renderDataTable(id, data, tableHeaders, temporalField, typeIndex) {
        var $table = $('#' + id + '-table');
        var $thead = $table.find('thead');
        var $tbody = $table.find('tbody');
        var html = '';
        var row = '';
        // Header row
        row += '<tr>';
        _.each(tableHeaders, function(header) {
            row += '<th>' + header + ' (' + typeIndex[header] + ')</th>\n';
        });
        row += '</tr>\n';
        $thead.html(row);
        // Only put 10 rows of data into the HTML table.
        data = data.slice(0, 10);
        _.each(data, function(point) {
            row = '<tr>';
            _.each(tableHeaders, function(header) {
                var dataType = typeIndex[header]
                var value = point[header]
                if (dataType == 'location' && value != undefined) {
                    value = value.coordinates.join(', ')
                }
                row += '<td>' + value + '</td>\n';
            });
            row += '</tr>\n';
            html += row;
        });
        $tbody.html(html);
    }

    function enableNavigationButtons() {
        // Enable navigation buttons
        var url = window.location.href;
        var splitUrl = url.split('/');
        var currentPage = parseInt(splitUrl.pop());
        var urlNoPage = splitUrl.join('/');
        var nextPage = currentPage + 1;
        var prevPage = currentPage - 1;
        var hasQuery = url.indexOf('?') > 0
        var prevUrl = urlNoPage + '/' + prevPage;
        var nextUrl = urlNoPage + '/' + nextPage;

        if (hasQuery) {
            prevUrl += '?' + url.split('?').pop();
            nextUrl += '?' + url.split('?').pop();
        }
        if (prevPage > -1) {
            $('.prev-nav-container').html('<a href="' + prevUrl + '">Page ' + prevPage + '</a>');
        }
        $('.next-nav-container').html('<a href="' + nextUrl + '">Page ' + nextPage + '</a>');
    }

    function renderVisualizations() {
        // Renders visualizations for each resource on the page.
        $('.viz').each(function(i, el) {
            var dataAttrs = $(el).data()
            var id = dataAttrs.id;
            var temporalField = dataAttrs.temporalField;
            var dataLimit = DEFAULT_DATA_LIMIT;
            if (_.contains(window.location.href, 'resource')) {
                dataLimit = SCALAR_DATA_LIMIT;
            }
            var typeIndex = {};
            var dataType = dataAttrs.type;
            if (dataType == 'geospatial') {
                dataLimit = GEO_DATA_LIMIT;
            }
            var jsonUrl = dataAttrs.jsonUrl
                + '?$order=' + temporalField + ' DESC'
                + '&$limit=' + dataLimit;

            if (dataAttrs.seriesId) {
                alert(id + ": " + dataAttrs.seriesId);
            }

            // The rest of the data attributes are field types.
            _.each(dataAttrs, function(value, name) {
                if (! _.contains(['id', 'jsonUrl', 'temporalField', 'type'], name)) {
                    typeIndex[name] = value;
                }
            });

            $.getJSON(jsonUrl, function(data) {
                var graphLabels, tableHeaders;
                // Reverse the data because it came in descending order.
                data = data.reverse();

                // The temporal field is always first because it contains the date
                graphLabels = [temporalField];
                tableHeaders = [temporalField];
                _.each(typeIndex, function(type, name) {
                    // We will graph all number types
                    if (_.contains(['int', 'float'], type)) {
                        graphLabels.push(name);
                    }
                    if (name != temporalField) {
                        tableHeaders.push(name);
                    }
                });

                if (dataType == 'scalar') {
                    graphData(id, data, graphLabels, temporalField, typeIndex);
                } else {
                    renderMap(id, data, temporalField);
                }
                renderDataTable(id, data, tableHeaders, temporalField, typeIndex);
            });
        });
    }

    enableNavigationButtons();
    renderVisualizations();

}());
