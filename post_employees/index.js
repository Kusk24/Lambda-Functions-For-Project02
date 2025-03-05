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
    //    - If you have a mapping template in API Gateway, it might pass the payload as event.payload
    //    - Otherwise, event might already be the object
    //    - Adjust to your usage
    const payload = event.payload || event;

    // 3) Destructure fields with defaults
    //    - Provide fallback defaults for gender, birth_date, hire_date if not specified
    const {
      emp_no,
      first_name,
      last_name,
      gender = 'M',
      birth_date = '1980-01-01',
      hire_date = '2020-01-01',
      dept_no,
      title,
      salary
    } = payload;

    // 4) Basic validations
    //    - If you require certain fields, check them here
    if (!emp_no || !first_name || !last_name) {
      return {
        statusCode: 400,
        body: JSON.stringify({ 
          error: "Missing required fields: emp_no, first_name, or last_name."
        })
      };
    }

    // Optional: ensure numeric fields are valid
    const numericEmpNo = Number(emp_no);
    if (isNaN(numericEmpNo) || numericEmpNo <= 0) {
      return {
        statusCode: 400,
        body: JSON.stringify({ 
          error: `Invalid emp_no: ${emp_no}`
        })
      };
    }
    const numericSalary = salary ? Number(salary) : null;
    if (salary && (isNaN(numericSalary) || numericSalary <= 0)) {
      return {
        statusCode: 400,
        body: JSON.stringify({ 
          error: `Invalid salary: ${salary}`
        })
      };
    }

    // 5) Insert into `employees`
    const insertEmpQuery = `
      INSERT INTO employees (emp_no, birth_date, first_name, last_name, gender, hire_date)
      VALUES (?, ?, ?, ?, ?, ?)
    `;
    await connection.execute(insertEmpQuery, [
      numericEmpNo,
      birth_date,
      first_name,
      last_name,
      gender,
      hire_date
    ]);

    // 6) Insert into `dept_emp` if dept_no is provided
    if (dept_no) {
      const insertDeptEmp = `
        INSERT INTO dept_emp (emp_no, dept_no, from_date, to_date)
        VALUES (?, ?, CURDATE(), '9999-01-01')
      `;
      await connection.execute(insertDeptEmp, [numericEmpNo, dept_no]);
    }

    // 7) Insert into `titles` if title is provided
    if (title) {
      const insertTitle = `
        INSERT INTO titles (emp_no, title, from_date, to_date)
        VALUES (?, ?, CURDATE(), '9999-01-01')
      `;
      await connection.execute(insertTitle, [numericEmpNo, title]);
    }

    // 8) Insert into `salaries` if salary is provided
    if (numericSalary) {
      const insertSalary = `
        INSERT INTO salaries (emp_no, salary, from_date, to_date)
        VALUES (?, ?, CURDATE(), '9999-01-01')
      `;
      await connection.execute(insertSalary, [numericEmpNo, numericSalary]);
    }

    // 9) Clear the memcached key for top employees
    try {
      await delAsync('top_employees');
      console.log('Flushed memcached key: top_employees');
    } catch (cacheError) {
      console.error('Memcache delete error:', cacheError);
    }

    // 10) Retrieve updated top employees
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

    // 11) Cache the fresh top employees
    try {
      await setAsync('top_employees', JSON.stringify(topRows), 300);
      console.log('Cached new top_employees for 5 minutes (300s).');
    } catch (cacheError) {
      console.error('Memcache set error:', cacheError);
    }

    // 12) Return success response
    return {
      statusCode: 201,
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        message: 'Employee created successfully',
        emp_no: numericEmpNo,
        topEmployees: topRows,
        cache_info: "Cache flushed and updated"
      })
    };
  } catch (error) {
    console.error('Error creating employee:', error);
    return {
      statusCode: 500,
      body: JSON.stringify({ 
        error: 'Failed to create employee',
        details: error.message 
      })
    };
  } finally {
    // 13) Close DB connection if open
    if (connection) {
      await connection.end();
    }
  }
};
