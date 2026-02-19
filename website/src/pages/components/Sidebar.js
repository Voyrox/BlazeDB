import Head from "next/head";

const Sidebar = () => {
  return (
    <><>
      <Head>
        <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.2.1/css/all.min.css" />
      </Head>
    </><nav className="sidebar">
        <header>
          <div className="image-text">
            <span className="image">
              <img src="/xeondb.png" alt="logo"></img>
            </span>

            <div className="text logo-text">
              <span className="name">BlazeDB</span>
            </div>
          </div>

          <i className='bx bx-chevron-right toggle'></i>
        </header>

        <div className="menu-bar">
          <div className="menu">

            <li className="search-box">
              <i className="fa fa-light fa-rocket"></i>
              <input type="text" placeholder=" Search Databases..."></input>
            </li>

            <ul className="menu-links">
              <li className="nav-link">
                <a href="#">
                  <i className="fa fa-desktop"></i>
                  <span className="text nav-text"> Projects</span>
                </a>
              </li>

              <li className="nav-link">
                <a href="#">
                  <i className="fa fa-solid fa-code-fork"></i>
                  <span className="text nav-text"> Organizations</span>
                </a>
              </li>

              <li className="nav-link">
                <a href="#">
                  <i className="fa fa-solid fa-shield"></i>
                  <span className="text nav-text"> Account</span>
                </a>
              </li>

              <li className="nav-link">
                <a href="#">
                  <i className="fa-solid fa-gear"></i>
                  <span className="text nav-text"> Documentation</span>
                </a>
              </li>
            </ul>
          </div>

          <div className="bottom-content">
            <li className="log">
              <a href="#">
                <i class="fa-solid fa-right-from-bracket"></i>
                <span className="text nav-text"> Logout</span>
              </a>
            </li>
          </div>
        </div>

      </nav></>
  );
};

export default Sidebar;