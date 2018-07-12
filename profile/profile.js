google.charts.load("current", {packages:["timeline"]});

function drawChart(){
    let startDate = new Date(document.getElementById('start').value)
    let duration = parseInt(document.getElementById('duration').value)
    let finishDate = startDate == undefined || duration == undefined ? undefined : new Date(startDate.valueOf() + duration)
    var index = document.getElementById('index').value;
    if (isNaN(startDate ))
	startDate = undefined;
    
    if (isNaN(finishDate ))
	finishDate = undefined;
    drawChart2(index, startDate, finishDate);
}

function getMinAndMaxDate() {
    var index = document.getElementById('index').value;
    let json = {
        aggs: {
            minDate : {
                min : {
                    field : "start"
                }
            },
            maxDate : {
                max : {
                    field : "finish"
                }
            }
        }
    }
    $.ajax({
    type: "POST",
	contentType: "application/json",
	dataType: "json",
	url: "http://localhost:9200/" + index + "/_search?size=0",
    data: JSON.stringify(json)
    }).done(results => {
        let minDate = document.getElementById("minDate")
        let maxDate = document.getElementById("maxDate")
        minDate.innerHTML = results["minDate"]
        maxDate.innerHTML = results["maxDate"]
    })

}

function drawChart2(index, startDate, finishDate) {

    var json = {
	size: 10000,
	sort: [
	    {hostname:{order:"asc"}},
	    {index:{order:"asc"}}
	],
	query: {
	    bool: {
		should: [
		    {
			range:{
			    start: {
				gte: startDate,
				lte: finishDate
			    }
			}
		    }, {
			range: {
			    finish: {
				gte: startDate,
				lte: finishDate
			    }
			}
		    }
		],
		minimum_should_match: 1
	    }
	}
    };
    
    $.ajax({
	type: "POST",
	contentType: "application/json",
	dataType: "json",
	url: "http://localhost:9200/" + index + "/_search",
	data: JSON.stringify(json)
    }).done(function(results){
	hits = results["hits"]["hits"].map(function(h){return h["_source"];});

	var container = document.getElementById('example3.1');
	var chart = new google.visualization.Timeline(container);
        var dataTable = new google.visualization.DataTable();
        dataTable.addColumn({ type: 'string', id: 'Position' });
        dataTable.addColumn({ type: 'string', id: 'Name' });
	dataTable.addColumn({ type: "string", role: "style"});
        dataTable.addColumn({ type: 'date', id: 'Start' });
        dataTable.addColumn({ type: 'date', id: 'End' });
	
	var timeline = [];
	var resources = new Set();
	var colorMap = {}
	colorMap["irods_capability_automated_ingest.sync_task.sync_file"] = '#ff8888';
	colorMap["irods_capability_automated_ingest.sync_task.sync_dir"] = '#88ff88';
	colorMap["irods_capability_automated_ingest.sync_task.sync_path"] = '#8888ff';
	colorMap["irods_capability_automated_ingest.sync_task.sync_restart"] = '#234783';
	console.log(hits)
	hits.forEach(function(obj){
	    var task_id = obj["event_id"]
	    var start=obj["start"]
	    var finish=obj["finish"]
	    var row = [obj["hostname"]+"/"+obj["index"], task_id, colorMap[obj["event_name"]], new Date(start), new Date(finish)];
	    dataTable.addRow(row);
	});
	
	chart.draw(dataTable, {
	    height:"100%",
	    width:"100%",
	    hAxis: {
		format: "MMM d, y HH:mm:ss",
		minValue: startDate,
		maxValue: finishDate
	    }
	});
    }).fail(function(a,b,c){
	console.log(b)
	console.log(c);
    });
}
