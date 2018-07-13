function drawChart(){
    let startDate = new Date(document.getElementById('start').value)
    if (isNaN(startDate))
    	startDate = undefined
    let duration = parseInt(document.getElementById('duration').value)
    let finishDate = startDate == undefined || duration == undefined ? undefined : new Date(startDate.valueOf() + duration)
    let index = document.getElementById('index').value;
    if (isNaN(finishDate))
	    finishDate = undefined
    drawChart2(index, startDate, finishDate);
}

function getMinAndMaxDate() {
    var index = document.getElementById('index').value
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

function getMin() {
    let minDate = document.getElementById("minDate")
    return minDate.innerHTML
}

function getMax() {
    let minDate = document.getElementById("maxDate")
    return minDate.innerHTML
}

function setStart(value) {
    let startDate = document.getElementById('start')
    startDate.value = value
}

function setFinish(value) {
    let startDate = document.getElementById('finish')
    startDate.value = value
}

function groupName(obj) {
    let index = obj["index"]
    let indexString = ""
    if (index < 10) {
	    indexString = "0" + index
    } else {
	    indexString = "" + index
    }
    return obj["hostname"]+"/" + indexString
}

function drawChart2(index, startDate, finishDate) {
    const batchsize = 10000
    const hits = []
    const json = {
        size: batchsize,
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
    }

    const handleResults = (sid, remaining, data) => {
        data.forEach(h => {
            hits.push(h["_source"])
            remaining--
        })
        if(remaining !== 0) {
            scroll(sid, remaining)
        } else {
            showTable(startDate, finishDate, hits)
        }
    }
    
    const scroll = (sid, remaining) => {
	let json = {
	    scroll: "1m",
	    scroll_id: sid
	}
	$.ajax({
        type: "POST",
        contentType: "application/json",
        dataType: "json",
        url: "http://localhost:9200/_search/scroll",
        data: JSON.stringify(json)
	}).done(results => {
	    handleResults(results["_scroll_id"], remaining, results["hits"]["hits"])
	}).fail((a,b,c) => {
        console.log(b)
        console.log(c)
	})
    }
    
    $.ajax({
        type: "POST",
        contentType: "application/json",
        dataType: "json",
        url: "http://localhost:9200/" + index + "/_search?scroll=1m",
        data: JSON.stringify(json)
    }).done(results => {
        const data = results["hits"]
        const total = data["total"]
        handleResults(results["_scroll_id"], total, data["hits"])
    }).fail((a,b,c) => {
        console.log(b)
        console.log(c)
    })
}

function showTable(startDate, finishDate, hits){
    const container = document.getElementById("visualization")
    const groupNames0 = new Set()

    hits.forEach(obj => {
        groupNames0.add(groupName(obj))
    })

    const groupNames = Array.from(groupNames0).sort()
    const groups = new vis.DataSet()
    const groupMap = {}
    for(let g = 0; g < groupNames.length; g++) {
        groups.add({id: g, content: groupNames[g]})
	    groupMap[groupNames[g]] = g
    }

    const colorMap = {}
    colorMap["irods_capability_automated_ingest.sync_task.sync_file"] = 'sync_file';
    colorMap["irods_capability_automated_ingest.sync_task.sync_dir"] = 'sync_dir';
    colorMap["irods_capability_automated_ingest.sync_task.sync_path"] = 'sync_path';
    colorMap["irods_capability_automated_ingest.sync_task.restart"] = 'restart';

    let count = hits.length
    document.getElementById("numEvents").innerHTML = count
    
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
            group: groupMap[groupName(obj)],
            content: task_id,
	    title: `${task_id}<br/>start: ${taskStartDate}<br/>finish: ${taskEndDate}`,
            start: taskStartDate,
            end: taskEndDate,
            className: colorMap[task_name]
        })
    })

    let options = {
        tooltip: {
            overflowMethod: "cap"
        },
        moveable: true,
        zoomable: true,
        selectable: false,
        showCurrentTime: false,
        stack: false,
        groupOrder: "content"
    }
    if(startDate !== undefined) {
	    options["min"] = startDate
	    options["start"] = startDate
	    options["end"] = new Date(startDate.valueOf() + 1000)
    }
    if(finishDate !== undefined) {
	    options["max"] = finishDate
    }
    
    container.innerHTML = ""
    let timeline = new vis.Timeline(container)
    timeline.setOptions(options)
    timeline.setGroups(groups)
    timeline.setItems(items)
}
