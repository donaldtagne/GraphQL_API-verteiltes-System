/*Importieren Sie das mysql2/promise Modul, das eine vollständige Promises-Schnittstelle für MySQL bietet.*/
const mysql = require('mysql2/promise');

// Konfiguration für die MySQL-Datenbankverbindung
const dbConfig = {
  host: 'localhost',
  user: 'root',
  password: 'hallochris',
  database: 'customerdb',
};

/*Erstellen Sie zwei Datenbank-Pools mit der gleichen Konfiguration.*/
const db = mysql.createPool(dbConfig);
const db_1 = mysql.createPool(dbConfig);

/*Flags zur Überwachung, ob die Tabellen bereits erstellt wurden.*/
let customerTableCreated = false;
let customer1TableCreated = false;

/*Diese Funktion generiert einen Array von Kundendaten für die 'customer'-Tabelle.*/
async function generateCustomers(numCustomers) {
  const customers = [];
  for (let i = 1; i <= numCustomers; i++) {
    const user = `user${i}`;
    const password = `password${i}`;
    const firstName = `First${i}`;
    const lastName = `Last${i}`;
    const address = `address${i}`;
    const email = `${user}@example.com`;
    const c_since = 1 + (i * 0.1);
    customers.push([i, user, password, firstName, lastName, address, email, c_since]);
  }
  return customers;
}
/*Diese Funktion generiert einen Array von Kundendaten für die 'customer_1'-Tabelle.*/
async function generateCustomers1(numCustomers) {
  const customers1 = [];
  for (let i = 1; i <= numCustomers; i++) {
    const id=i;
    const last_login = `2022-06-${String((i % 30) + 1).padStart(2, '0')}`;;
    const login = `user${i}`;
    const expiration = `2023-06-${String((i % 30) + 1).padStart(2, '0')}`;
    const discount = (i * 0.1).toFixed(1);
    const balance = String(i * 100);
    const ytd_pmt = String(i * 1000);
    const birthdate = `1990-${String((i % 12) + 1).padStart(2, '0')}-10`;
    customers1.push([id, last_login, login, expiration, discount, balance, ytd_pmt, birthdate]);
  }
  return customers1;
}
/*Diese Funktion erstellt die 'customer'-Tabelle und füllt sie mit Daten.*/
async function createCustomerTable() {
  if (customerTableCreated) {
    console.log('Tabelle "customer" existiert bereits.');
    return;
  }

  try {
    await db.query(`
    CREATE TABLE IF NOT EXISTS customer (
      C_id INT PRIMARY KEY,
      C_UNAME VARCHAR(255) NOT NULL,
      C_PASSWD VARCHAR(255) UNIQUE NOT NULL,
      C_FNAME VARCHAR(255) NOT NULL,
      C_LNAME VARCHAR(255) NOT NULL,
      C_ADDR_ID VARCHAR(255) UNIQUE NOT NULL,
      C_EMAIL VARCHAR(255) UNIQUE NOT NULL,
      C_SINCE FLOAT NOT NULL
    )
    `);
      /*Generieren Sie die Kundendaten.*/
    const customers = await generateCustomers(100);
    /* Fügen Sie jeden Kunden einzeln in die Tabelle ein.*/
    for (const customer of customers) {
      try {
        await db.query(`
          INSERT INTO customer (C_id, C_UNAME, C_PASSWD, C_FNAME, C_LNAME, C_ADDR_ID, C_EMAIL, C_SINCE)
          VALUES ( ?, ?, ?, ?, ?, ?, ?, ?)
        `, customer);
      } catch (err) {
        console.error('Error inserting customer data:', err);
      }
    }

    customerTableCreated = true;
    console.log('Customer table created and data inserted successfully.');
  } catch (err) {
    console.error('Error creating customer table and inserting data:', err);
    throw err;
  }
}

async function createCustomer1Table() {
  if (customer1TableCreated) {
    console.log('Tabelle "customer_1" existiert bereits. Keine weiteren Schritte erforderlich.');
    return;
  }

  try {
    await db_1.query(`
    CREATE TABLE IF NOT EXISTS customer_1 (
      C_id INT PRIMARY KEY,
      C_LAST_LOGIN DATE NOT NULL,
      C_LOGIN VARCHAR(255)  NOT NULL,
      C_EXPIRATION DATE NOT NULL,
      C_DISCOUNT DECIMAL(10, 2) NOT NULL,
      C_BALANCE DECIMAL(10, 2)  NOT NULL,
      C_YTD_PMT DECIMAL(10, 2)  NOT NULL,
      C_BIRTHDATE DATE NOT NULL
    )
    `);

    const customers1 = await generateCustomers1(100);
    for (const customer of customers1) {
      try {
        await db_1.query(`
          INSERT INTO customer_1 (C_id, C_LAST_LOGIN, C_LOGIN, C_EXPIRATION, C_DISCOUNT, C_BALANCE, C_YTD_PMT, C_BIRTHDATE)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        `, customer);
      } catch (err) {
        console.error('Error inserting customer_1 data:', err);
      }
    }

    customer1TableCreated = true;
    console.log('Customer_1 table created and data inserted successfully.');
  } catch (err) {
    console.error('Error creating customer_1 table and inserting data:', err);
    throw err;
  }
}

Promise.all([createCustomerTable(), createCustomer1Table()])
  .then(() => {
    console.log('Database tables created and data inserted successfully.');
  })
  .catch((err) => {
    console.error('Error creating database tables and inserting data:', err);
  });

  
 /* Exportieren Sie die Datenbank-Pools  Modulen.*/
module.exports = { db, db_1 };

