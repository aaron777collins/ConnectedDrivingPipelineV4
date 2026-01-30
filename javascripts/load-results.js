let currentPage;
let pageSize;
let resultsNavigation;
let inProgressJobsNavigation;

document.addEventListener("DOMContentLoaded", function () {
  console.log("Works!");
  pageSize = 3; // number of results per page

  resultsNavigation = new Pagination("nav-buttons", 1, pageSize, 1);
  inProgressJobsNavigation = new Pagination("in-progress-jobs-nav-buttons", 1, pageSize, 1);

  $(document).on('click', '.chart-button', function() {
    var chartId = $(this).data('chart');
    $('#container-' + chartId).toggle();
  });

  loadAllResults();
});

const slugify = (str) =>
  str
    .toLowerCase()
    .trim()
    .replace(/[^\w\s-]/g, "")
    .replace(/[\s_-]+/g, "-")
    .replace(/^-+|-+$/g, "");

function loadAllResults() {
  let inProgressJobsUrl =
    "https://docs.google.com/spreadsheets/d/e/2PACX-1vQ3-YLEzjlg7QLghAER39VFa1JiIUmLFZ3PmwAPpn_h44PPcqvy2mdSUA7Ze7Vjk7CDYHe4VNl0sE8s/pub?gid=387020741&single=true&output=csv";

  // loading results for in progress jobs
  loadResults(inProgressJobsUrl, "in-progress-jobs", "in-progress-results-content", (data, _id) => {

    let allRows = data.split(/\r?\n|\r/);
    let names = [];
    let descriptions = [];
    let links = [];
    let authors = [];
    let dates = [];
    for (let singleRow = 1; singleRow < allRows.length; singleRow++) {
      let rowCells = allRows[singleRow].split(",");
      names.push(rowCells[0]);
      descriptions.push(rowCells[1]);
      links.push(rowCells[2]);
      authors.push(rowCells[3]);
      dates.push(rowCells[4]);
    }

    for (let arr of [names, descriptions, links, authors, dates]) {
      arr.reverse();
    }

    // Compute total pages
    let totalPages = Math.ceil(names.length / pageSize);
    inProgressJobsNavigation.totalPages = totalPages;
    inProgressJobsNavigation.createNavigationButtons();

    // Clear existing results
    $("#in-progress-results-content").empty();

    if (names.length === 0) {
      $("#in-progress-results-content").append("<p>No jobs are running at the moment.</p>");
    }

    // Loop only for current page items
    for (let i = inProgressJobsNavigation.start; i < inProgressJobsNavigation.end && i < names.length; i++) {
      let name = names[i];
      let link = links[i];
      let description = descriptions[i];
      let author = authors[i];
      let date = dates[i];
      id = slugify(name + "-" + author + "-" + date + "-" + link);
      $("#in-progress-results-content").append(
        "<div class='result-container' id='" + id + "'></div>"
      );
      console.log("Link URL", link);
      $("#" + id).append(
        "<a href='" + link + "'>" + "<h3>" + name + "</h3>" + "</a>"
      );
      $("#" + id).append("<p>" + author + " | " + date + "</p>");
      $("#" + id).append("<p>" + description + "</p>");
    }

  });

  let linksUrl =
    "https://docs.google.com/spreadsheets/d/e/2PACX-1vQ3-YLEzjlg7QLghAER39VFa1JiIUmLFZ3PmwAPpn_h44PPcqvy2mdSUA7Ze7Vjk7CDYHe4VNl0sE8s/pub?gid=0&single=true&output=csv";
  loadResults(linksUrl, "getting-links", "results-content", (data, _id) => {
    let allRows = data.split(/\r?\n|\r/);
    let names = [];
    let descriptions = [];
    let links = [];
    let authors = [];
    let dates = [];
    for (let singleRow = 1; singleRow < allRows.length; singleRow++) {
      let rowCells = allRows[singleRow].split(",");
      names.push(rowCells[0]);
      descriptions.push(rowCells[1]);
      links.push(rowCells[2]);
      authors.push(rowCells[3]);
      dates.push(rowCells[4]);
    }

    for (let arr of [names, descriptions, links, authors, dates]) {
      arr.reverse();
    }

    // Compute total pages
    let totalPages = Math.ceil(names.length / pageSize);
    resultsNavigation.totalPages = totalPages;
    resultsNavigation.createNavigationButtons();

    // Clear existing results
    $("#results-content").empty();

    if (names.length === 0) {
      $("#results-content").append("<p>No results found.</p>");
    }

    // Loop only for current page items
    console.log(resultsNavigation)
    for (let i = resultsNavigation.start; i < resultsNavigation.end && i < names.length; i++) {
      let name = names[i];
      let link = links[i];
      let description = descriptions[i];
      let author = authors[i];
      let date = dates[i];
      id = slugify(name + "-" + author + "-" + date + "-" + link);
      $("#results-content").append(
        "<div class='result-container' id='" + id + "'></div>"
      );
      console.log("Link URL", link);
      $("#" + id).append(
        "<a href='" + link + "'>" + "<h3>" + name + "</h3>" + "</a>"
      );
      $("#" + id).append("<p>" + author + " | " + date + "</p>");
      $("#" + id).append("<p>" + description + "</p>");
      $("#" + id).append(
        "<p class='loading-text'><i class='fas fa-spinner fa-spin'></i> Loading...</p>"
      );
      loadResults(link, id, "results-content", (data, id) => {
        $("#" + id)
          .find("p:last")
          .remove();
        writeTableFromResults(data, (id = id));
      });
    }
  });
}

function loadResults(url, id, resultsID, success) {
  results = $.ajax({
    url: url,
    dataType: "text",
    cache: false,
    success: successFunction,
    error: errorFunction,
  });

  function successFunction(data) {
    hideLoading();
    success(data, id);
  }

  function errorFunction(e) {
    hideLoading();
    console.log("Error loading results", e);
    $("#" + resultsID).append("Error loading results");
  }
}

function hideLoading() {
  $("#loading-results").hide();
}

function writeTableFromResults(data, id = "results-content") {
  console.log(id);
  // find anything with data:image..., and replace the comma ',' with !%! so that it doesn't get split
  data = data.replace(/data:image[^,]+,/g, function (match) {
    return match.replace(/,/g, "!$!");
  });

  // replace any commas within {} with !%$*$%! so that it doesn't get split
  data = data.replace(/{[^}]+}/g, function (match) {
    return match.replace(/,/g, "!%$*$%!");
  });

  var allRows = data.split(/\r?\n|\r/);
  var table =
    "<table class='result-table result-table-no-links custom-scroll-bar'>";

  const CHARTABLE_HEADERS = [
    "train_accuracy",
    "train_precision",
    "train_recall",
    "train_f1",
    "train_specificity",
    "test_accuracy",
    "test_precision",
    "test_recall",
    "test_f1",
    "test_specificity",
  ];

  let chartData = {
    models: [],
  };

  for (let header of CHARTABLE_HEADERS) {
    chartData[header] = [];
  }

  for (var singleRow = 0; singleRow < allRows.length; singleRow++) {
    if (singleRow === 0) {
      table += "<thead>";
      table += "<tr>";
    } else {
      table += "<tr>";
    }
    var rowCells = allRows[singleRow].split(",");
    for (var rowCell = 0; rowCell < rowCells.length; rowCell++) {
      // check if the cell is in double quotes
      if (
        rowCells[rowCell].startsWith('"') &&
        rowCells[rowCell].endsWith('"')
      ) {
        rowCells[rowCell] = rowCells[rowCell].substring(
          1,
          rowCells[rowCell].length - 1
        );
      }


      // Here's where we collect the data for our charts.
      if (allRows[0].split(",")[rowCell] === "Model") {
        chartData.models.push(rowCells[rowCell]);
      } else {
        for (let header of CHARTABLE_HEADERS) {
          if (allRows[0].split(",")[rowCell] === header) {
            chartData[header].push(parseFloat(rowCells[rowCell]));
          }
        }
      }

      // replace !%$*$%! with ,
      rowCells[rowCell] = rowCells[rowCell].replace(/!\%\$\*\$\%\!/g, ",");

      if (singleRow === 0) {
        table += "<th class='result-header-cell'>";
        table += rowCells[rowCell];
        table += "</th>";
      } else {
        table += "<td class='result-body-cell'>";

        // check if it is a base64 image
        if (rowCells[rowCell].startsWith("data:image")) {
          // replace !$! with ,
          rowCells[rowCell] = rowCells[rowCell].replace(/!\$\!/g, ",");
          imageData = "";
          // loop through this cell and all afterwards until it is a blank cell or the end of the csv
          // we want to merge the remnants of the image
          for (var i = rowCell; i < rowCells.length; i++) {
            if (rowCells[i] === "") {
              break;
            }
            imageData += rowCells[i];
            // set the current cell data (and all after) to blank
            rowCells[i] = "";
          }

          table +=
            "<img class='clickable-image' id='clickable-image-" +
            rowCell +
            "' src='" +
            imageData +
            "' onclick='showImageModal(this.src)'>";
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
  $("#" + id).append(table);

  // search for empty column at the end of the table and remove it
  // this is because the csv has image data
  // we don't want to show this column if all cells in the column are empty
  $("table tr").each(function () {
    // run recursively until we find a cell with content
    // remove the last cell if it is empty
    while (
      $(this).find("td:last").text() === "" &&
      $(this).find("td:last").html() === ""
    ) {
      $(this).find("td:last").remove();
    }
    // remove the last header cell if it is empty
    while (
      $(this).find("th:last").text() === "" &&
      $(this).find("th:last").html() === ""
    ) {
      $(this).find("th:last").remove();
    }
  });

  // looping through the chart data and removing the column info from the "Model" column
  // First, finding the index of the model column
  var modelColumnIndex = chartData.models.indexOf("Model");
  // if the model column exists, remove it from the chart data
  if (modelColumnIndex > -1) {
    chartData.models.splice(modelColumnIndex, 1);
    for (let header of CHARTABLE_HEADERS) {
      chartData[header].splice(modelColumnIndex, 1);
    }
  }

  // createChart creates charts based on the data only if the data exists

  for (let header of CHARTABLE_HEADERS) {
    if (chartData[header].length > 0) {
      createChart(
        id,
        id + "chart-" + header,
        header,
        chartData.models,
        chartData[header]
      );
    }
  }
}

// This function creates a bar chart with the given parameters.
function createChart(divID, canvasId, chartTitle, labels, data) {
  // Only create chart if data array is not empty
  if (data.length > 0) {
    // remove all elements in the labels and data once an empty string is found
    // this is because the csv has other data

    // find the first empty string in the labels
    var firstEmptyLabel = labels.indexOf("");
    // if there is an empty string, remove all elements after it
    if (firstEmptyLabel > -1) {
      labels.splice(firstEmptyLabel, labels.length - firstEmptyLabel);
      data.splice(firstEmptyLabel, data.length - firstEmptyLabel);
    }

    // Create the button for showing the chart and a div to contain the chart.

    // $("#" + divID).append("<button class='chart-button' data-chart='" + canvasId + "'>" + chartTitle + "</button>");
    // $("#" + divID).append("<div class='chart-container' id='container-" + canvasId + "' style='display: none;'><canvas id='" + canvasId + "'></canvas></div>");


    // Create the button for showing the chart and a div to contain the chart.
    var chartDiv = document.createElement('div');
    chartDiv.className = 'chart';
    chartDiv.innerHTML = `<button class="toggleButton" onclick="toggleChart(this, '${divID}-${canvasId}')">${chartTitle} ▼</button>
                          <div id="${divID}-${canvasId}" style="display:none">
                            <canvas id="${canvasId}"></canvas>
                          </div>`;
    document.getElementById(divID).appendChild(chartDiv);

    // // Create the canvas element
    // $("#" + divID).append("<canvas id='" + canvasId + "'></canvas>");
    var ctx = document.getElementById(canvasId).getContext("2d");

    // Generate random color for the chart
    var red = Math.floor(Math.random() * 256);
    var green = Math.floor(Math.random() * 256);
    var blue = Math.floor(Math.random() * 256);
    var backgroundColor = "rgba(" + red + ", " + green + ", " + blue + ", 0.2)";

    // Generate darker color for the border
    var darkerRed = Math.max(red - 25, 0);
    var darkerGreen = Math.max(green - 25, 0);
    var darkerBlue = Math.max(blue - 25, 0);
    var borderColor =
      "rgba(" + darkerRed + ", " + darkerGreen + ", " + darkerBlue + ", 1)";

    new Chart(ctx, {
      type: "bar",
      data: {
        labels: labels,
        datasets: [
          {
            label: chartTitle,
            data: data,
            backgroundColor: backgroundColor,
            borderColor: borderColor,
            borderWidth: 1,
          },
        ],
      },
      options: {
        scales: {
          y: {
            beginAtZero: true,
            max: 1,
          },
        },
        plugins: {
          legend: {
            position: "bottom",
          },
        },
      },
    });
  }

}

function toggleChart(button, canvasId) {
  var chart = document.getElementById(canvasId);
  if(chart.style.display == 'none') {
    chart.style.display = 'block';
    button.innerHTML = button.innerHTML.replace('▼', '▲');
  } else {
    chart.style.display = 'none';
    button.innerHTML = button.innerHTML.replace('▲', '▼');
  }
}

function writeTableFromResultsWithLinks(data) {
  var allRows = data.split(/\r?\n|\r/);
  var table =
    "<table class='result-table result-table-with-links custom-scroll-bar'>";
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
          table +=
            "<a href='" + rowCells[rowCell] + "'>" + rowCells[rowCell] + "</a>";
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

function showImageModal(src) {
  // if called for the first time, add the #image-modal div to the body, along with the #image-modal-content img
  if ($("#image-modal").length == 0) {
    $("body").append("<div id='image-modal'></div>");
    $("#image-modal").append("<img id='image-modal-content'>");
    $("#image-modal").click(function () {
      $("#image-modal").hide();
    });
  }

  $("#image-modal").show();
  $("#image-modal-content").attr("src", src);
}
