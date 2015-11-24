$(function() {
    $('.viz').each(function(i, el) {
        var data = $(el).data()
        var id = data.id;
        var temporalField = data.temporalField;
        var jsonUrl = data.jsonUrl + '?$order=' + temporalField + ' DESC';
        var typeIndex = {};
        // The rest of the data attributes are field types.
        _.each(data, function(value, name) {
            if (! _.contains(['id', 'jsonUrl', 'temporalField'], name)) {
                typeIndex[name] = value;
            }
        });
        $.getJSON(jsonUrl, function(data) {
            var graphInput = [];
            // The temporal field is always first because it contains the date
            var graphLabels = [temporalField];
            _.each(typeIndex, function(type, name) {
                // We will graph all number types
                if (_.contains(['int', 'float'], type)) {
                    graphLabels.push(name);
                }
            });
            // Reverse the data because it came in descending order.
            data = data.reverse();
            _.each(data, function(point) {
                var row = [];
                row.push(new Date(point[temporalField]));
                for (var i = 1; i < graphLabels.length; i++) {
                    row.push(new Number(point[graphLabels[i]]))
                }
                graphInput.push(row);
            });
            new Dygraph(
                document.getElementById(id + "-viz"),
                graphInput, 
                {labels: graphLabels}
            );
        });
    });
}());