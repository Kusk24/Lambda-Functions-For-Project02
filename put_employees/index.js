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

    // 2) Parse the event payload.
    //    - If using a mapping template, it might be event.payload
    //    - If using Lambda Proxy Integration, parse event.body
    //    - Otherwise, fall back to event itself
    let payload;
    if (event.payload) {
      payload = event.payload;
    } else if (event.body) {
      payload = JSON.parse(event.body);
    } else {
      payload = event;
    }

    // 3) Destructure fields from payload
    const {
      emp_no,
      first_name,
      last_name,
      birth_date,
      gender,
      hire_date,
      dept_no,
      title,
      salary
    } = payload;

    // Validate emp_no
    if (!emp_no) {
      return {
        statusCode: 400,
        body: JSON.stringify({ error: 'Missing emp_no for update' })
      };
    }

    const numericEmpNo = Number(emp_no);
    if (isNaN(numericEmpNo) || numericEmpNo <= 0) {
      return {
        statusCode: 400,
        body: JSON.stringify({ error: `Invalid emp_no: ${emp_no}` })
      };
    }

    // 4) Update `employees` table if any employee field is provided
    if (first_name || last_name || birth_date || gender || hire_date) {
      let updateEmpQuery = `UPDATE employees SET `;
      const fields = [];
      const params = [];

      if (first_name) {
        fields.push('first_name = ?');
        params.push(first_name);
      }
      if (last_name) {
        fields.push('last_name = ?');
        params.push(last_name);
      }
      if (birth_date) {
        fields.push('birth_date = ?');
        params.push(birth_date);
      }
      if (gender) {
        fields.push('gender = ?');
        params.push(gender);
      }
      if (hire_date) {
        fields.push('hire_date = ?');
        params.push(hire_date);
      }

      updateEmpQuery += fields.join(', ');
      updateEmpQuery += ` WHERE emp_no = ?`;
      params.push(numericEmpNo);

      await connection.execute(updateEmpQuery, params);
    }

    // 5) Update or Insert into `dept_emp` if `dept_no` is provided
    if (dept_no) {
      // Check if there's an active dept_emp record for this employee
      const [existingDept] = await connection.execute(
        `SELECT * FROM dept_emp WHERE emp_no = ? AND to_date = '9999-01-01'`,
        [numericEmpNo]
      );
      if (existingDept.length > 0) {
        // Update the current active department record
        await connection.execute(
          `UPDATE dept_emp SET dept_no = ? WHERE emp_no = ? AND to_date = '9999-01-01'`,
          [dept_no, numericEmpNo]
        );
      } else {
        // Insert a new record if none exists
        await connection.execute(
          `INSERT INTO dept_emp (emp_no, dept_no, from_date, to_date)
           VALUES (?, ?, CURDATE(), '9999-01-01')`,
          [numericEmpNo, dept_no]
        );
      }
    }

    // 6) Update or Insert into `titles` if `title` is provided
    if (title) {
      const [existingTitle] = await connection.execute(
        `SELECT * FROM titles WHERE emp_no = ? AND to_date = '9999-01-01'`,
        [numericEmpNo]
      );
      if (existingTitle.length > 0) {
        await connection.execute(
          `UPDATE titles SET title = ? WHERE emp_no = ? AND to_date = '9999-01-01'`,
          [title, numericEmpNo]
        );
      } else {
        await connection.execute(
          `INSERT INTO titles (emp_no, title, from_date, to_date)
           VALUES (?, ?, CURDATE(), '9999-01-01')`,
          [numericEmpNo, title]
        );
      }
    }

    // 7) Update or Insert into `salaries` if `salary` is provided
// 7) Update or Insert into `salaries` if `salary` is provided and not null
if (salary !== null && salary !== undefined && salary !== "") {
  const numericSalary = Number(salary);
  if (isNaN(numericSalary) || numericSalary <= 0) {
    return {
      statusCode: 400,
      body: JSON.stringify({ error: `Invalid salary: ${salary}` })
    };
  }
  const [existingSalary] = await connection.execute(
    `SELECT * FROM salaries WHERE emp_no = ? AND to_date = '9999-01-01'`,
    [numericEmpNo]
  );
  if (existingSalary.length > 0) {
    await connection.execute(
      `UPDATE salaries SET salary = ? WHERE emp_no = ? AND to_date = '9999-01-01'`,
      [numericSalary, numericEmpNo]
    );
  } else {
    await connection.execute(
      `INSERT INTO salaries (emp_no, salary, from_date, to_date)
       VALUES (?, ?, CURDATE(), '9999-01-01')`,
      [numericEmpNo, numericSalary]
    );
  }
} else {
  // Salary is null or empty, so skip updating the salary field.
  console.log("No new salary provided; retaining the old salary.");
}


    // 8) Invalidate any relevant cache entries
    try {
      const empCacheKey = `employee_${numericEmpNo}`;
      await delAsync(empCacheKey);
      console.log(`Flushed cache for key: ${empCacheKey}`);
      await delAsync('top_employees');
      console.log('Flushed cache for key: top_employees');
    } catch (cacheError) {
      console.error('Memcache delete error:', cacheError);
    }

    // 9) Re-query top employees for consistency with post/delete
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

    // 10) Cache the fresh top employees
    try {
      await setAsync('top_employees', JSON.stringify(topRows), 30);
      console.log('Cached new top_employees for 5 minutes (30s).');
    } catch (cacheError) {
      console.error('Memcache set error:', cacheError);
    }

    // 11) Return success response
    return {
      statusCode: 200,
      body: JSON.stringify({
        message: 'Employee updated successfully',
        emp_no: numericEmpNo,
        topEmployees: topRows
      })
    };
  } catch (error) {
    console.error('Error updating employee:', error);
    return {
      statusCode: 500,
      body: JSON.stringify({ 
        error: 'Failed to update employee',
        details: error.message 
      })
    };
  } finally {
    if (connection) {
      await connection.end();
    }
  }
};
