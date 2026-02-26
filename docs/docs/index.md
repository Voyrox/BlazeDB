<div class="home">

  <section class="home-hero">
    <div class="home-hero__text">
      <h1>Welcome to XeonDB Documentation</h1>

      <p class="home-lead">Search for commands, config, drivers, and examples. Tip: press <code>s</code> on any page to open search.</p>

      <div class="home-search" role="search" aria-label="Search docs">
        <button class="home-search__btn" type="button" data-bs-toggle="modal" data-bs-target="#mkdocs_search_modal">
          <i class="fa fa-search" aria-hidden="true"></i>
          <span class="home-search__text">Search documentation...</span>
          <span class="home-search__hint"><kbd>s</kbd></span>
        </button>
      </div>

    </div>

    <div class="home-hero__art">
      <div class="home-logoCard" aria-hidden="true">
        <img class="home-logo" src="img/logo.png" alt="" />
      </div>
    </div>
  </section>

  <section class="home-section">
    <h2>Pick a path</h2>
    <div class="home-grid">
      <a class="home-card" href="quickstart">
        <div class="home-card__title">Quick Start</div>
        <div class="home-card__body">Build, run, and send a few queries over TCP.</div>
        <div class="home-card__link">Open Quick Start</div>
      </a>

      <a class="home-card" href="query-examples">
        <div class="home-card__title">Query examples</div>
        <div class="home-card__body">Schema, reads, scans with ORDER BY, updates, deletes.</div>
        <div class="home-card__link">See examples</div>
      </a>

      <a class="home-card" href="drivers">
        <div class="home-card__title">Drivers</div>
        <div class="home-card__body">Official Node.js and Go clients with simple request/response flow.</div>
        <div class="home-card__link">Open Drivers</div>
      </a>

      <a class="home-card" href="deployment">
        <div class="home-card__title">Deployment</div>
        <div class="home-card__body">Native binary or Docker, plus durability + config notes.</div>
        <div class="home-card__link">Open Deployment</div>
      </a>
    </div>
  </section>

  <section class="home-section">
    <h2>Common tasks</h2>
    <div class="home-grid home-grid--2">
      <a class="home-card" href="quickstart/#build">
        <div class="home-card__title">Build + run</div>
        <div class="home-card__body">Build with Ninja, start the server, and run the first smoke checks.</div>
        <div class="home-card__link">Open Quick Start</div>
      </a>

      <a class="home-card" href="drivers/#nodejs">
        <div class="home-card__title">Connect a client</div>
        <div class="home-card__body">Install the Node.js driver (or Go) and send requests from your app.</div>
        <div class="home-card__link">Open Drivers</div>
      </a>
    </div>
  </section>

  <section class="home-section">
    <h2>Core concepts</h2>
    <div class="home-grid home-grid--2">
      <div class="home-card home-card--static">
        <div class="home-card__title">SQL subset</div>
          <div class="home-card__body">
          PING, AUTH, USE, CREATE, INSERT, SELECT, UPDATE, DELETE, FLUSH, SHOW, DESCRIBE, DROP, TRUNCATE, ORDER BY, GROUP BY, MIN/MAX/COUNT/SUM/AVG.
          </div>
      </div>

      <div class="home-card home-card--static">
        <div class="home-card__title">Write path</div>
        <div class="home-card__body">
          Append to WAL, stage in memory, flush to sorted tables, compact in the background.
        </div>
      </div>
    </div>
  </section>

  <section class="home-section">
    <h2>Need help?</h2>
    <div class="home-grid home-grid--2">
      <a class="home-card" href="https://github.com/Voyrox/Xeondb/issues/new/choose">
        <div class="home-card__title">Open an issue</div>
        <div class="home-card__body">Bug reports, questions, and feature requests are tracked on GitHub.</div>
        <div class="home-card__link">Go to Issues</div>
      </a>

      <a class="home-card" href="https://github.com/Voyrox/Xeondb">
        <div class="home-card__title">Browse the repo</div>
        <div class="home-card__body">Source code, releases, and driver packages live in the main repository.</div>
        <div class="home-card__link">Open GitHub</div>
      </a>
    </div>
  </section>

</div>
