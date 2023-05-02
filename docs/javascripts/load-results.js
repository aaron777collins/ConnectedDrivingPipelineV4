document.addEventListener("DOMContentLoaded", function () {
  console.log("Works!");

  loadAllResults();
});

function zip(a, b) {
    var arr = [];
    for (var key in a) arr.push([a[key], b[key]]);
    return arr;
}

function loadAllResults() {

    let linksUrl = "https://docs.google.com/spreadsheets/d/e/2PACX-1vQ3-YLEzjlg7QLghAER39VFa1JiIUmLFZ3PmwAPpn_h44PPcqvy2mdSUA7Ze7Vjk7CDYHe4VNl0sE8s/pub?output=csv";

    loadResults(linksUrl, (data) => {
        writeTableFromResultsWithLinks(data);
        // parse out the name of the models and the links in the second column (excluding the header row)
        let allRows = data.split(/\r?\n|\r/);
        let names = [];
        let links = [];
        for (let singleRow = 1; singleRow < allRows.length; singleRow++) {
            let rowCells = allRows[singleRow].split(",");
            names.push(rowCells[0]);
            links.push(rowCells[1]);
        }

        console.log("Links", links);
        for (let res of zip(names, links)) {
            let name = res[0];
            let link = res[1];
            console.log("Link URL", link);
            // add h3 with link name and url
            $("#results-content").append("<a href='" + link + "'>" + "<h3>" + name + "</h3>" + "</a>");
            loadResults(link, (data) => {
                writeTableFromResults(data);
            });
        }
    });
}

function loadResults(url, success) {

  results = $.ajax({
    url: url,
    dataType: "text",
    cache: false,
    success: successFunction,
    error: errorFunction,
  });

  function successFunction(data) {
    console.log("Success loading results", data);
    hideLoading();
    success(data);
  }

  function errorFunction(e) {
    hideLoading();
    console.log("Error loading results", e);
    $("#results-content").append("Error loading results");
  }
}

function hideLoading() {
  $("#loading-results").hide();
}

function writeTableFromResults(data) {
  var allRows = data.split(/\r?\n|\r/);
  var table = "<table class='result-table result-table-no-links'>";
  for (var singleRow = 0; singleRow < allRows.length; singleRow++) {
    if (singleRow === 0) {
      table += "<thead>";
      table += "<tr>";
    } else {
      table += "<tr>";
    }
    var rowCells = allRows[singleRow].split(",");
    for (var rowCell = 0; rowCell < rowCells.length; rowCell++) {
      if (singleRow === 0) {
        table += "<th class='result-header-cell'>";
        table += rowCells[rowCell];
        table += "</th>";
      } else {
        table += "<td class='result-body-cell'>";
        table += rowCells[rowCell];
        table += "</td>";
      }
    }
    if (singleRow === 0) {
      table += "</tr>";
      table += "</thead>";
      table += "<tbody>";
    } else {
      table += "</tr>";
    }
  }
  table += "</tbody>";
  table += "</table>";
  hideLoading();
  $("#results-content").append(table);
}

function writeTableFromResultsWithLinks(data) {

    var allRows = data.split(/\r?\n|\r/);
    var table = "<table class='result-table result-table-with-links'>";
    for (var singleRow = 0; singleRow < allRows.length; singleRow++) {
        if (singleRow === 0) {
        table += "<thead>";
        table += "<tr>";
        } else {
        table += "<tr>";
        }
        var rowCells = allRows[singleRow].split(",");
        for (var rowCell = 0; rowCell < rowCells.length; rowCell++) {
        if (singleRow === 0) {
            table += "<th class='result-header-cell-with-links'>";
            table += rowCells[rowCell];
            table += "</th>";
        } else {
            // if it is a link, make it a link.
            // otherwise, just write the text
            table += "<td class='result-body-cell-with-links'>";

            if (rowCells[rowCell].startsWith("http")) {
                table += "<a href='" + rowCells[rowCell] + "'>" + rowCells[rowCell] + "</a>";
            } else {
                table += rowCells[rowCell];
            }

            table += "</td>";
        }
        }
        if (singleRow === 0) {
        table += "</tr>";
        table += "</thead>";
        table += "<tbody>";
        } else {
        table += "</tr>";
        }
    }
    table += "</tbody>";
    table += "</table>";
    hideLoading();
    $("#results-content").append(table);


}
