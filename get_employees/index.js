const mysql = require('mysql2/promise');
const Memcached = require('memcached');
const util = require('util');

const dbConfig = {
  host: 'project02-rds.cbk4kwa002dq.us-east-1.rds.amazonaws.com',
  user: 'admin',
  password: 'coffee_beans_for_all',
  database: 'employees'
};

// Hard-coded memcache endpoint
const memcacheEndpoint = 'project02.hcv3pm.cfg.use1.cache.amazonaws.com:11211';
const memcached = new Memcached(memcacheEndpoint);

// Promisify memcached methods for easier async/await usage
const getAsync = util.promisify(memcached.get).bind(memcached);
const setAsync = util.promisify(memcached.set).bind(memcached);

exports.handler = async (event) => {
  let connection;
  try {
    console.log('Lambda function started');
    console.log('Event:', JSON.stringify(event));
    
    connection = await mysql.createConnection(dbConfig);

    // Check if we have a path parameter, e.g., /employees/{emp_no}
    const emp_no = event.pathParameters?.emp_no;
    let query;
    let params = [];
    
    // Define a cache key based on the query type
    const cacheKey = emp_no ? `employee_${emp_no}` : `top_employees`;

    // Attempt to get the result from memcache
    let cachedData;

    // Set TTL to 30 seconds for all queries
    const cacheTTL = 30; 
    try {
      console.log(`Attempting to retrieve from cache with key: ${cacheKey}`);
      cachedData = await getAsync(cacheKey);
      
      if (cachedData) {
        console.log('Cache HIT for key:', cacheKey);
        return {
          statusCode: 200,
          headers: { 'Content-Type': 'application/json' },
          body: cachedData // cachedData is assumed to be a JSON string
        };
      } else {
        console.log('Cache MISS for key:', cacheKey);
      }
    } catch (cacheError) {
      console.error('Memcache get error:', cacheError);
    }

    if (emp_no) {
      // Fetch a single employee
      query = `SELECT * FROM employees WHERE emp_no = ?`;
      params = [emp_no];
    } else {
      // Fetch the top 10 employees as per your complex query
      query = `
        SELECT e.emp_no, e.first_name, e.last_name, d.dept_name, MAX(s.salary) AS max_salary 
        FROM employees e 
        JOIN dept_emp de ON e.emp_no = de.emp_no 
        JOIN departments d ON de.dept_no = d.dept_no 
        JOIN (
          SELECT emp_no, salary 
          FROM salaries 
          WHERE to_date = '9999-01-01'
        ) s ON e.emp_no = s.emp_no 
        WHERE s.salary > (SELECT AVG(salary) FROM salaries) 
        GROUP BY e.emp_no, e.first_name, e.last_name, d.dept_name 
        ORDER BY max_salary DESC 
        LIMIT 10;
      `;
    }

    // Execute the SQL query
    console.log('Executing database query');
    const [rows] = await connection.execute(query, params);
    const responseBody = JSON.stringify(rows);

    // Cache the response for 30 seconds
    try {
      console.log(`Caching result for key: ${cacheKey}`);
      await setAsync(cacheKey, responseBody, cacheTTL);
      console.log(`Successfully cached data for key: ${cacheKey}`);
    } catch (cacheError) {
      console.error('Memcache set error:', cacheError);
    }

    return {
      statusCode: 200,
      headers: { 'Content-Type': 'application/json' },
      body: responseBody
    };
  } catch (error) {
    console.error('Error retrieving employees:', error);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: 'Failed to retrieve employees' })
    };
  } finally {
    if (connection) {
      await connection.end();
    }
    console.log('Lambda function completed');
  }
};