const mysql = require('mysql2/promise');
const Memcached = require('memcached');
const util = require('util');

// Database connection info
const dbConfig = {
  host: 'project02-rds.cbk4kwa002dq.us-east-1.rds.amazonaws.com',
  user: 'admin',
  password: 'coffee_beans_for_all',
  database: 'employees'
};

// Memcached endpoint
const memcacheEndpoint = 'project02.hcv3pm.cfg.use1.cache.amazonaws.com:11211';
const memcached = new Memcached(memcacheEndpoint);

// Promisify Memcached methods
const setAsync = util.promisify(memcached.set).bind(memcached);
const delAsync = util.promisify(memcached.del).bind(memcached);

exports.handler = async (event) => {
  let connection;
  try {
    // 1) Connect to MySQL
    connection = await mysql.createConnection(dbConfig);

    // 2) Parse the event payload
    //    - If you have a mapping template, it might pass the payload as event.payload
    //    - Otherwise, event might be the direct object
    const payload = event.payload || event;

    // 3) Extract emp_no and validate
    const { emp_no } = payload;
    if (!emp_no) {
      return {
        statusCode: 400,
        body: JSON.stringify({ error: 'Missing emp_no for delete' })
      };
    }

    const numericEmpNo = Number(emp_no);
    if (isNaN(numericEmpNo) || numericEmpNo <= 0) {
      return {
        statusCode: 400,
        body: JSON.stringify({ error: `Invalid emp_no: ${emp_no}` })
      };
    }

    // 4) Delete from referencing tables
    await connection.execute(`DELETE FROM dept_emp WHERE emp_no = ?`, [numericEmpNo]);
    await connection.execute(`DELETE FROM titles WHERE emp_no = ?`, [numericEmpNo]);
    await connection.execute(`DELETE FROM salaries WHERE emp_no = ?`, [numericEmpNo]);
    await connection.execute(`DELETE FROM dept_manager WHERE emp_no = ?`, [numericEmpNo]);

    // 5) Delete from employees
    await connection.execute(`DELETE FROM employees WHERE emp_no = ?`, [numericEmpNo]);

    // 6) Clear the memcached key for top employees
    try {
      await delAsync('top_employees');
      console.log('Flushed memcached key: top_employees');
    } catch (cacheError) {
      console.error('Memcache delete error:', cacheError);
    }

    // 7) Retrieve updated top employees (same logic as your POST function)
    const topEmployeesQuery = `
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
    const [topRows] = await connection.execute(topEmployeesQuery);

    // 8) Cache the fresh top employees
    try {
      await setAsync('top_employees', JSON.stringify(topRows), 300);
      console.log('Cached new top_employees for 5 minutes (300s).');
    } catch (cacheError) {
      console.error('Memcache set error:', cacheError);
    }

    // 9) Return success response
    return {
      statusCode: 200,
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        message: 'Employee deleted successfully',
        emp_no: numericEmpNo,
        topEmployees: topRows,
        cache_info: "Cache flushed and updated"
      })
    };
  } catch (error) {
    console.error('Error deleting employee:', error);
    return {
      statusCode: 500,
      body: JSON.stringify({ 
        error: 'Failed to delete employee',
        details: error.message 
      })
    };
  } finally {
    // 10) Close DB connection if open
    if (connection) {
      await connection.end();
    }
  }
};