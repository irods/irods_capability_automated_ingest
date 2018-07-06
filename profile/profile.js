google.charts.load("current", {packages:["timeline"]});
google.charts.setOnLoadCallback(drawChart);
function drawChart() {
    $.ajax({
	dataType: "text",
	url: "profile"}).done(function(results){
	    console.log(results); 
	    var container = document.getElementById('example3.1');
	    var chart = new google.visualization.Timeline(container);
            var dataTable = new google.visualization.DataTable();
            dataTable.addColumn({ type: 'string', id: 'Position' });
            dataTable.addColumn({ type: 'string', id: 'Name' });
	    dataTable.addColumn({ type: "string", role: "style"});
            dataTable.addColumn({ type: 'date', id: 'Start' });
            dataTable.addColumn({ type: 'date', id: 'End' });
	    
	    var timeline = [];
	    var task_buf = {};
	    var resources = new Set();
	    var lines = results.split("\n");
	    var rows = [];
	    var colorMap = {}
	    colorMap["irods_capability_automated_ingest.sync_task.sync_file"] = '#ff8888';
	    colorMap["irods_capability_automated_ingest.sync_task.sync_dir"] = '#88ff88';
	    colorMap["irods_capability_automated_ingest.sync_task.sync_path"] = '#8888ff';
	    colorMap["irods_capability_automated_ingest.sync_task.sync_restart"] = '#234783';
	    lines.forEach(function(line){
		if(line !== "") {
		    var obj = JSON.parse(line)

		    var task_id = obj["task_id"]
		    console.log(obj)
		    var buf = task_buf[task_id]
		    if (buf === undefined)
			task_buf[task_id] = obj
		    else {
			delete task_buf[task_id]
			if (obj["event"] === "task_prerun") {
			    var start=obj["@timestamp"]
			    var finish=buf["@timestamp"]
			} else {
			    var start=buf["@timestamp"]
			    var finish=obj["@timestamp"]
			}
			var row = [obj["hostname"]+"/"+obj["index"], obj["task_id"], colorMap[obj["task_name"]], new Date(start), new Date(finish)];
			console.log(row);
			dataTable.addRow(row);
			task_name = obj["task_name"]
			rows.push(task_name)
		    }
		}

	    });

	    chart.draw(dataTable, {height:"100%", width:"100%"});
	});
}
