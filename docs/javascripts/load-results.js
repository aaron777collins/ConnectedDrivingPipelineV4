document.addEventListener("DOMContentLoaded", function () {
  console.log("Works!");

//   add event listener to a button with the title "Switch to light more"
//   when clicked, it makes css changes

  loadAllResults();
});

const slugify = str =>
  str
    .toLowerCase()
    .trim()
    .replace(/[^\w\s-]/g, '')
    .replace(/[\s_-]+/g, '-')
    .replace(/^-+|-+$/g, '');

function loadAllResults() {

    let linksUrl = "https://docs.google.com/spreadsheets/d/e/2PACX-1vQ3-YLEzjlg7QLghAER39VFa1JiIUmLFZ3PmwAPpn_h44PPcqvy2mdSUA7Ze7Vjk7CDYHe4VNl0sE8s/pub?output=csv";

    loadResults(linksUrl, (data) => {
        writeTableFromResultsWithLinks(data);
        // parse out the name of the models and the links in the second column (excluding the header row)
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
          // reverse the order of the arrays
          arr.reverse();
        }

        console.log("Links", links);
        for (let name of names) {
            // create div of class "result-container" with unique id of name, author, date and link
            let idx = names.indexOf(name);
            let link = links[idx];
            let description = descriptions[idx];
            let author = authors[idx];
            let date = dates[idx];
            id = slugify(name + "-" + author + "-" + date + "-" + link);
            $("#results-content").append("<div class='result-container' id='" + id + "'></div>"); // add id to div of "name-author-date-link
            console.log("Link URL", link);
            // add h3 with link name and url
            $("#" + id).append("<a href='" + link + "'>" + "<h3>" + name + "</h3>" + "</a>");
            // add author and date
            $("#" + id).append("<p>" + author + " | " + date + "</p>");
            // add description
            $("#" + id).append("<p>" + description + "</p>");
            loadResults(link, (data) => {
                writeTableFromResults(data, id=id);
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

function writeTableFromResults(data, id="results-content") {
  // find anything with data:image..., and replace the comma ',' with !%! so that it doesn't get split
  data = data.replace(/data:image[^,]+,/g, function (match) {
    return match.replace(/,/g, "!$!");
  });
  var allRows = data.split(/\r?\n|\r/);
  var table = "<table class='result-table result-table-no-links custom-scroll-bar'>";
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
      if (rowCells[rowCell].startsWith('"') && rowCells[rowCell].endsWith('"')) {
        rowCells[rowCell] = rowCells[rowCell].substring(1, rowCells[rowCell].length - 1);
      }
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
          imageData = ""
          // loop through this cell and all afterwards until it is a blank cell or the end of the csv
          // we want to merge the remnants of the image
          for (var i = rowCell; i < rowCells.length; i++) {
            if(rowCells[i] === "") {
              break;
            }
            imageData += rowCells[i];
          }

          table += "<img class='clickable-image' id='clickable-image-" + rowCell + "' src='" + imageData + "' onclick='showImageModal(this.src)'>";
          // we found the image so we are done rendering for this row (we don't want to render the rest of the cells in this row)
          // end cell and break
          table += "</td>";
          break;
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
}

function writeTableFromResultsWithLinks(data) {

    var allRows = data.split(/\r?\n|\r/);
    var table = "<table class='result-table result-table-with-links custom-scroll-bar'>";
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
