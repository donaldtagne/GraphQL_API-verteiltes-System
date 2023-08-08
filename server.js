const { ApolloServer, gql } = require('apollo-server');
const {db, db_1} = require('./datenbank.js');

const typeDefs = gql`
  type kunde {
    C_id: Int!
    C_UNAME: String!
    C_PASSWD: String!
    C_FNAME: String!
    C_LNAME: String!
    C_ADDR_ID: String!
    C_EMAIL: String!
    C_SINCE: Float!
    C_LAST_LOGIN: Date!
    C_LOGIN: String!
    C_EXPIRATION: Date!
    C_DISCOUNT: Float!
    C_BALANCE: Float!
    C_YTD_PMT: Float!
    C_BIRTHDATE: Date!
  }
  scalar Date
  type Query {
    customers: [kunde]
  }
  type AddCustomersResult {
    insertedCustomers: Int!
  }
  
  type Mutation {
    addCustomers(numCustomers: Int!): AddCustomersResult
  }
`;

async function generateCustomers(numCustomers) {
  try {
    const [results] = await db.query('SELECT * FROM customer');
    
    const customers = [];
    for(let i = 1; i <= numCustomers; i++) {
      const id = i;
      const user = `user${id}`;
      const password = `password${id}`;
      const firstName = `First${id}`;
      const lastName = `Last${id}`;
      const address = `address${id}`;
      const email = `${user}@example.com`;
      const c_since = 1 + (i * 0.1);
      const last_login = `2022-06-${String((i % 30) + 1).padStart(2, '0')}`;;
      const login = user;
      const expiration = `2023-06-${String((i % 30) + 1).padStart(2, '0')}`;
      const discount = (i * 0.1).toFixed(1);
      const balance = String(i * 100);
      const ytd_pmt = String(i * 1000);
      const birthdate = `1990-${String((i % 12) + 1).padStart(2, '0')}-10`;
      const customerData = [id, user, password, firstName, lastName, address, email, c_since, last_login, login, expiration, discount, balance, ytd_pmt, birthdate];
      customers.push(customerData);
    }
    return customers;
  } catch (err) {
    console.error('Error generating customers:', err);
    throw err;
  }
}
const resolvers = {
  Query: {
    customers: async () => {
      console.time("Database Query"); /*Start des Timers*/
      try {
        const [combinedData] = await db.query(`
          SELECT 
          customer.C_id AS C_id,
          customer.C_UNAME AS C_UNAME,
          customer.C_PASSWD AS C_PASSWD,
          customer.C_FNAME AS C_FNAME,
          customer.C_LNAME AS C_LNAME,
          customer.C_ADDR_ID AS C_ADDR_ID,
          customer.C_EMAIL AS C_EMAIL,
          customer.C_SINCE AS C_SINCE,
          customer_1.C_LAST_LOGIN AS C_LAST_LOGIN,
          customer_1.C_LOGIN as C_LOGIN,
          customer_1.C_EXPIRATION AS C_EXPIRATION,
          customer_1.C_DISCOUNT AS C_DISCOUNT,
          customer_1.C_BALANCE AS C_BALANCE,
          customer_1.C_YTD_PMT AS C_YTD_PMT,
          customer_1.C_BIRTHDATE AS C_BIRTHDATE
        FROM 
          customer 
        JOIN 
          customer_1  ON customer.C_id = customer_1.C_id
        `);
    
        if (!combinedData || combinedData.length === 0) {
          console.error('Fehler beim Abrufen der Kunden aus der Datenbank oder ungültige C_id-Werte.');
          return [];
        }
        console.timeEnd("Database Query");  /*Ende des Timers*/
        
        return combinedData;
      } catch (err) {
        console.error('Fehler beim Abrufen der Kunden aus der Datenbank:', err);
        return [];
      }
    },
  },
  Mutation: {
    addCustomers: (_, { numCustomers }) => {
      return new Promise(async (resolve, reject) => {
        /*Generiert customers*/
        const customers = await generateCustomers(numCustomers);
  
        /*Build SQL queries*/
        const deleteQueryCustomer = 'DELETE FROM customer';
        const deleteQueryCustomer_1 = 'DELETE FROM customer_1';
        const insertQueryCustomer = `
          INSERT INTO customer (C_id, C_UNAME, C_PASSWD, C_FNAME, C_LNAME, C_ADDR_ID, C_EMAIL, C_SINCE)
          VALUES ?
        `;
        const insertQueryCustomer_1 = `
          INSERT INTO customer_1 (C_id, C_LAST_LOGIN, C_LOGIN, C_EXPIRATION, C_DISCOUNT, C_BALANCE, C_YTD_PMT, C_BIRTHDATE)
          VALUES ?
        `;
  
        /* Prepare data for each table */
        const customerData = customers.map(customer => [customer[0], customer[1], customer[2], customer[3], customer[4], customer[5], customer[6], customer[7]]);
        const customer1Data = customers.map(customer => [customer[0], customer[8], customer[9], customer[10], customer[11], customer[12], customer[13], customer[14]]);
        
        /*Fügt neue customers in der  datenbank*/
        console.time("Database Query"); /*Start der Zeitmessung*/
        
        try {
          /* delete entries from customer */
          await db.query(deleteQueryCustomer);
          /* delete entries from customer_1 */
          await db_1.query(deleteQueryCustomer_1);
          /* insert into customer */
          await db.query(insertQueryCustomer, [customerData]);
          /* insert into customer_1 */
          await db_1.query(insertQueryCustomer_1, [customer1Data]);
          console.timeEnd("Database Query");
          resolve({ insertedCustomers: numCustomers });
        } catch (err) {
          reject(err);
        }
      });
    },
  }
};  

const server = new ApolloServer({ typeDefs, resolvers });

server.listen({ port: 4000 }).then(({ url }) => {
  console.log(`GraphQL server running at ${url}`);
});
