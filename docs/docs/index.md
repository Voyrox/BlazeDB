<div class="home">

  <section class="home-hero">
    <div class="home-hero__text">
      <h1>Welcome to XeonDB Documentation</h1>
      <p class="home-lead">
        A lightweight, high-performance NoSQL database that is scalable and durable.
      </p>

      <div class="home-cta">
        <a class="btn btn-primary" href="quickstart.md">Getting started</a>
        <a class="btn btn-outline-light" href="deployment.md">Deploy</a>
        <a class="btn btn-link home-cta__link" href="https://github.com/Voyrox/Xeondb">GitHub</a>
      </div>

    </div>

    <div class="home-hero__art">
      <img src="img/banner.png" alt="XeonDB banner" />
    </div>
  </section>

  <section class="home-section">
    <h2>Getting started</h2>
    <div class="home-grid">
      <a class="home-card" href="quickstart.md">
        <div class="home-card__title">First steps</div>
        <div class="home-card__body">Build, run locally, and send your first queries.</div>
        <div class="home-card__link">Open Quick Start</div>
      </a>

      <a class="home-card" href="deployment.md">
        <div class="home-card__title">Deployment</div>
        <div class="home-card__body">Run natively or via Docker and keep data durable.</div>
        <div class="home-card__link">Open Deployment</div>
      </a>

      <a class="home-card" href="quickstart.md#try-queries">
        <div class="home-card__title">Query examples</div>
        <div class="home-card__body">Create tables, insert rows, scan with ORDER BY.</div>
        <div class="home-card__link">See examples</div>
      </a>
    </div>
  </section>

  <section class="home-section">
    <h2>Core concepts</h2>
    <div class="home-grid home-grid--2">
      <div class="home-card home-card--static">
        <div class="home-card__title">SQL subset</div>
        <div class="home-card__body">
          PING, USE, CREATE, INSERT, SELECT, UPDATE, DELETE, FLUSH, ORDER BY.
        </div>
      </div>

      <div class="home-card home-card--static">
        <div class="home-card__title">Protocol</div>
        <div class="home-card__body">
          One SQL statement per line over TCP, one JSON response per line.
        </div>
      </div>
    </div>
  </section>

</div>
