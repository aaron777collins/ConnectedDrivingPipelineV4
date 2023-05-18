let currentPage;
let pageSize;

document.addEventListener("DOMContentLoaded", function () {
  console.log("Works!");

  // initialize the first page
  currentPage = 1;
  pageSize = 3; // number of results per page

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

  loadResults(linksUrl, "getting-links", (data, _id) => {
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

      // Clear existing results
      $("#results-content").empty();

      let start = (currentPage - 1) * pageSize;
      let end = start + pageSize;

      // Loop only for current page items
      for (let i = start; i < end && i < names.length; i++) {
          let name = names[i];
          let link = links[i];
          let description = descriptions[i];
          let author = authors[i];
          let date = dates[i];
          id = slugify(name + "-" + author + "-" + date + "-" + link);
          $("#results-content").append("<div class='result-container' id='" + id + "'></div>");
          console.log("Link URL", link);
          $("#" + id).append("<a href='" + link + "'>" + "<h3>" + name + "</h3>" + "</a>");
          $("#" + id).append("<p>" + author + " | " + date + "</p>");
          $("#" + id).append("<p>" + description + "</p>");
          loadResults(link, id, (data, id) => {
              writeTableFromResults(data, id=id);
          });
      }

      createNavigationButtons(totalPages);
  });
}

function loadResults(url, id, success) {
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
      $("#results-content").append("Error loading results");
  }
}

// Navigation Buttons
// Navigation Buttons
function createNavigationButtons(totalPages) {
  const navButtonsContainer = $('#nav-buttons');
  navButtonsContainer.empty(); // Clear existing buttons

  // First and Previous buttons
  const firstButton = $('<button class="material-button" id="first-page-button"><i class="fas fa-backward"></i></button>');
  const prevButton = $('<button class="material-button" id="prev-page-button"><i class="fas fa-chevron-left"></i></button>');
  firstButton.on('click', function() {
    currentPage = 1;
    loadAllResults();
  });
  prevButton.on('click', function() {
    currentPage--;
    loadAllResults();
  });
  navButtonsContainer.append(firstButton, prevButton);

  // Page number buttons
  let startPage = Math.max(1, currentPage - 2);
  let endPage = Math.min(totalPages, currentPage + 2);
  for (let i = startPage; i <= endPage; i++) {
    let pageButton = $(`<button class="material-button page-button ${i === currentPage ? 'active' : ''}">${i}</button>`);
    pageButton.on('click', function() {
      currentPage = parseInt(this.innerText);
      loadAllResults();
    });
    navButtonsContainer.append(pageButton);
  }

  // Next and Last buttons
  const nextButton = $('<button class="material-button" id="next-page-button"><i class="fas fa-chevron-right"></i></button>');
  const lastButton = $('<button class="material-button" id="last-page-button"><i class="fas fa-forward"></i></button>');
  nextButton.on('click', function() {
    currentPage++;
    loadAllResults();
  });
  lastButton.on('click', function() {
    currentPage = totalPages;
    loadAllResults();
  });
  navButtonsContainer.append(nextButton, lastButton);

  // Hide or show buttons based on the current page
  $("#first-page-button, #prev-page-button").prop("disabled", currentPage === 1);
  $("#next-page-button, #last-page-button").prop("disabled", currentPage === totalPages);
}





function hideLoading() {
  $("#loading-results").hide();
}

function writeTableFromResults(data, id="results-content") {
  console.log(id);
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
            // set the current cell data (and all after) to blank
            rowCells[i] = "";
          }

          table += "<img class='clickable-image' id='clickable-image-" + rowCell + "' src='" + imageData + "' onclick='showImageModal(this.src)'>";
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
    while($(this).find("td:last").text() === "" && $(this).find("td:last").html() === "") {
      $(this).find("td:last").remove();
    }
    // remove the last header cell if it is empty
    while($(this).find("th:last").text() === "" && $(this).find("th:last").html() === "") {
      $(this).find("th:last").remove();
    }

  });


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
