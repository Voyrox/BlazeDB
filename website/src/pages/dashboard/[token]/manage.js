import { useRouter } from 'next/router';
import { useState, useEffect } from 'react';
import Head from "next/head";
import Sidebar from "../../components/Sidebar";
import styles from '@/styles/Dashboard.module.css';
import axios from 'axios';


const Manage = () => {
  const router = useRouter();
  const { token } = router.query;

  const [RootData, setRootData] = useState(null);
  const [schemaData, setSchemaData] = useState(null);

  useEffect(() => {
    async function fetchData() {
      if (token) {
        try {
          const data = [
            {
              "id": "1",
              "name": "Jake",
              "age": 19,
              "PastNames": [
                "John",
                "Johnny",
                "Johnathan"
              ]
            },
            {
              "id": "2",
              "name": "Mike",
              "age": 20,
              "PastNames": [
                "Mich",
                "Mickel",
                "jake"
              ]
            },
            {
              "id": "3",
              "name": "Mike",
              "age": 20,
              "PastNames": [
                "Mich",
                "Mickel",
                "jake",
                "Mickel",
                "jake"
              ]
            },
            {
              "id": "4",
              "name": "Mike",
              "age": 20,
              "PastNames": [
                "Mich",
                "Mickel",
                "jake"
              ]
            },
            {
              "id": "5",
              "name": "Mike",
              "age": 20,
              "PastNames": [
                "Mich",
                "Mickel",
                "jake"
              ]
            },
            {
              "id": "6",
              "name": "Mike",
              "age": 20,
              "PastNames": [
                "Mich",
                "Mickel",
                "jake"
              ]
            }
          ];

          const schema = {
            id: { type: String, default: null },
            name: { type: String, default: null },
            age: { type: Number, default: null },
            PastNames: { type: Array, default: [] },
          };
          setSchemaData(schema);
          setRootData(data);
        } catch (error) {
          console.log(error);
        }
      }
    }
    fetchData();
  }, [token]);

  const createCard = () => {
    return (
      <>
       <div className={styles.gridcontainer}>
        {RootData?.map((item, index) => (
          <div key={item.id} className={styles.card}>
            <div className={styles.headerSchema}>
              <h2>Record {index + 1}:
                <div className={styles.cardButtons && styles.right}>
                  <button className={styles.selectButton}>Select</button>
                  <button className={styles.editButton}>Edit</button>
                  <button className={styles.deleteButton}>Delete</button>
                </div>
              </h2>
            </div>
            <br />
            <div className={styles.bodySchema}>
                <div className="middle">
                  <div key={item.id} className={styles.card && styles.right}>
                    <div className={styles.bodySchema}>
                      {Object.keys(schemaData).map((key) => {
                        const value = Array.isArray(item[key]) ? item[key].join(", ") : item[key];
                        return (
                          <div key={key} className={styles.field}>
                            <strong>{key}: </strong>
                            <span>{value}</span>
                          </div>
                        );
                      })}
                    </div>
                  </div>
                </div>
                <br></br>
                <div className={styles.right}>
                  <pre>{JSON.stringify(schemaData, null, 2)}</pre>
                </div>
            </div>
          </div>
        ))}
        </div>
      </>
    );
  };

  return (
    <>
      <Head>
        <title>Dashboard</title>
        <meta name="description" content="Generated by create next app" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <link rel="icon" href="/favicon.ico" />
      </Head>

      <div className={styles.container}>
        <nav>
          <Sidebar />
        </nav>
        <main className={styles.main}>
          <div className={styles.contentWrapper}>
            {createCard()}
          </div>

        </main>
      </div>
    </>
  );
};

export default Manage;