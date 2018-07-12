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
        minDate.innerHTML = results["aggregations"]["minDate"]["value_as_string"]
        maxDate.innerHTML = results["aggregations"]["maxDate"]["value_as_string"]
    })

}

function groupName(obj) {
    return obj["hostname"]+"/"+obj["index"]
}

function drawChart2(index, startDate, finishDate) {

    const json = {
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
    }).done(results => {
        const hits = results["hits"]["hits"].map(h => h["_source"])
        const container = document.getElementById("visualization")
        const groupNames0 = new Set()

        hits.forEach(obj => {
            groupNames0.add(groupName(obj))
        });

        const groupNames = Array.from(groupNamesSet).sort()
        const groups = new vis.DataSet()
        for(let g = 0; g < groupNames.length; g++) {
            groups.add({id: g, content: groupNames[g]})
        }

        const colorMap = {}
        colorMap["irods_capability_automated_ingest.sync_task.sync_file"] = 'sync_file';
        colorMap["irods_capability_automated_ingest.sync_task.sync_dir"] = 'sync_dir';
        colorMap["irods_capability_automated_ingest.sync_task.sync_path"] = 'sync_path';
        colorMap["irods_capability_automated_ingest.sync_task.restart"] = 'restart';

        const items = new vis.DataSet()
        hits.forEach((obj, index) => {
            let task_id = obj["event_id"]
            let task_name = obj["event_name"]
            let start=obj["start"]
            let finish=obj["finish"]
            let taskStartDate = new Date(start)
            let taskEndDate = new Date(finish)
            items.add({
                id: index,
                group: groupName(obj),
                content: task_id,
                start: taskStartDate,
                end: taskEndDate,
                className: colorMap[task_name]
            })
        });

        let timeline = new vis.Timeline(container)
        timeline.setGroups(groups)
        timeline.setItems(items)
    }).fail((a,b,c) => {
        console.log(b)
        console.log(c)
    });
}
