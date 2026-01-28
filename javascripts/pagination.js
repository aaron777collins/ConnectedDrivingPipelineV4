class Pagination {
  constructor(containerID, currentPage, resultsPerPage, totalPages) {
    this.containerID = containerID;
    this.currentPage = 1;
    this.resultsPerPage = resultsPerPage;
    this.totalPages = totalPages;
    this.getInstance = function () {
      return this;
    };
  }

  // Navigation Buttons
  // Navigation Buttons
  createNavigationButtons() {
    const navButtonsContainer = $("#" + this.containerID);
    navButtonsContainer.empty(); // Clear existing buttons

    // First and Previous buttons
    const firstButton = $(
      '<button class="material-button" id="' +
        this.containerID +
        '-first-page-button"><i class="fas fa-backward"></i></button>'
    );
    const prevButton = $(
      '<button class="material-button" id="' +
        this.containerID +
        '-prev-page-button"><i class="fas fa-chevron-left"></i></button>'
    );
    firstButton.on("click", { context: this.getInstance() }, function (e) {
      e.data.context.currentPage = 1;
      loadAllResults();
    });
    prevButton.on("click", { context: this.getInstance() }, function (e) {
      e.data.context.currentPage--;
      loadAllResults();
    });
    navButtonsContainer.append(firstButton, prevButton);

    // Page number buttons
    this.startPage = Math.max(1, this.currentPage - 2);
    this.endPage = Math.min(this.totalPages, this.currentPage + 2);

    this.start = (this.currentPage - 1) * this.resultsPerPage;
    this.end = this.start + this.resultsPerPage;

    for (let i = this.startPage; i <= this.endPage; i++) {
      let pageButton = $(
        `<button class="material-button page-button ${
          i === this.currentPage ? "active" : ""
        }">${i}</button>`
      );
      pageButton.on("click", { context: this.getInstance() }, function (e) {
        e.data.context.currentPage = parseInt(this.innerText);
        loadAllResults();
      });
      navButtonsContainer.append(pageButton);
    }

    // Next and Last buttons
    const nextButton = $(
      '<button class="material-button" id="' +
        this.containerID +
        '-next-page-button"><i class="fas fa-chevron-right"></i></button>'
    );
    const lastButton = $(
      '<button class="material-button" id="' +
        this.containerID +
        '-last-page-button"><i class="fas fa-forward"></i></button>'
    );
    nextButton.on("click", { context: this.getInstance() }, function (e) {
      e.data.context.currentPage++;
      loadAllResults();
    });
    lastButton.on("click", { context: this.getInstance() }, function (e) {
      e.data.context.currentPage = e.data.context.totalPages;
      loadAllResults();
    });
    navButtonsContainer.append(nextButton, lastButton);

    // Hide or show buttons based on the current page
    $(
      "#" +
        this.containerID +
        "-first-page-button, #" +
        this.containerID +
        "-prev-page-button"
    ).prop("disabled", this.currentPage === 1);
    $(
      "#" +
        this.containerID +
        "-next-page-button, #" +
        this.containerID +
        "-last-page-button"
    ).prop("disabled", this.currentPage === this.totalPages);
  }
}
