const mysql = require('mysql2/promise');
const Memcached = require('memcached');
const util = require('util');

const dbConfig = {
  host: 'project02.cbk4kwa002dq.us-east-1.rds.amazonaws.com',
  user: 'admin',
  password: 'Ggwp512512?',
  database: 'employees'
};

// Hard-coded memcache endpoint
const memcacheEndpoint = 'memcachedcache.hcv3pm.cfg.use1.cache.amazonaws.com:11211';
const memcached = new Memcached(memcacheEndpoint);

// Promisify memcached methods
const setAsync = util.promisify(memcached.set).bind(memcached);
const delAsync = util.promisify(memcached.del).bind(memcached);

exports.handler = async (event) => {
  let connection;
  try {
    connection = await mysql.createConnection(dbConfig);

    // Parse JSON body
    const body = JSON.parse(event.body);
    const {
      emp_no,
      first_name,
      last_name,
      gender = 'M',          // default
      birth_date = '1980-01-01',
      hire_date = '2020-01-01',
      dept_no,
      title,
      salary
    } = body;

    // 1) Insert into `employees`
    const insertEmpQuery = `
      INSERT INTO employees (emp_no, birth_date, first_name, last_name, gender, hire_date)
      VALUES (?, ?, ?, ?, ?, ?)
    `;
    await connection.execute(insertEmpQuery, [
      emp_no,
      birth_date,
      first_name,
      last_name,
      gender,
      hire_date
    ]);

    // 2) Insert into `dept_emp` if a department is provided
    if (dept_no) {
      const insertDeptEmp = `
        INSERT INTO dept_emp (emp_no, dept_no, from_date, to_date)
        VALUES (?, ?, CURDATE(), '9999-01-01')
      `;
      await connection.execute(insertDeptEmp, [emp_no, dept_no]);
    }

    // 3) Insert into `titles` if a title is provided
    if (title) {
      const insertTitle = `
        INSERT INTO titles (emp_no, title, from_date, to_date)
        VALUES (?, ?, CURDATE(), '9999-01-01')
      `;
      await connection.execute(insertTitle, [emp_no, title]);
    }

    // 4) Insert into `salaries` if a salary is provided
    if (salary) {
      const insertSalary = `
        INSERT INTO salaries (emp_no, salary, from_date, to_date)
        VALUES (?, ?, CURDATE(), '9999-01-01')
      `;
      await connection.execute(insertSalary, [emp_no, salary]);
    }

    // Flush (delete) the top employees cache because the data is now stale.
    try {
      await delAsync('top_employees');
      console.log('Flushed cache key: top_employees');
    } catch (cacheError) {
      console.error('Memcache delete error:', cacheError);
    }

    // Retrieve the updated top 10 employees (similar to your get_employees function)
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

    // Cache the fresh top employees data with a TTL of 300 seconds (5 minutes)
    try {
      await setAsync('top_employees', JSON.stringify(topRows), 300);
      console.log('Cached top employees under key: top_employees');
    } catch (cacheError) {
      console.error('Memcache set error:', cacheError);
    }

    // Return the top 10 employees along with a success message.
    return {
      statusCode: 201,
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        message: 'Employee created successfully',
        emp_no,
        topEmployees: topRows,
        cache_info: "Cache flushed and updated"
      })
    };
  } catch (error) {
    console.error('Error creating employee:', error);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: 'Failed to create employee' })
    };
  } finally {
    if (connection) {
      await connection.end();
    }
  }
};
