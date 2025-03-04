const mysql = require('mysql2/promise');

const dbConfig = {
  host: 'project02.cbk4kwa002dq.us-east-1.rds.amazonaws.com',
  user: 'admin',
  password: 'Ggwp512512?',
  database: 'employees'
};

exports.handler = async (event) => {
  let connection;
  try {
    connection = await mysql.createConnection(dbConfig);

    // Parse JSON body
    // Example body:
    // {
    //   "emp_no": 500000,
    //   "first_name": "UpdatedName",
    //   "last_name": "UpdatedLastName",
    //   "birth_date": "1970-01-01",
    //   "gender": "F",
    //   "hire_date": "2010-01-01",
    //   "dept_no": "d002",
    //   "title": "Senior Engineer",
    //   "salary": 80000
    // }
    const body = JSON.parse(event.body || '{}');
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
    } = body;

    if (!emp_no) {
      return {
        statusCode: 400,
        body: JSON.stringify({ error: 'Missing emp_no for update' })
      };
    }

    // 1) Update `employees` table if any employee field is provided
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
      params.push(emp_no);

      await connection.execute(updateEmpQuery, params);
    }

    // 2) Update or Insert into `dept_emp` if `dept_no` is provided
    if (dept_no) {
      // Check if there is an active dept_emp record for the employee
      const [existingDept] = await connection.execute(
        `SELECT * FROM dept_emp WHERE emp_no = ? AND to_date = '9999-01-01'`,
        [emp_no]
      );
      if (existingDept.length > 0) {
        // Update the current active department record
        await connection.execute(
          `UPDATE dept_emp SET dept_no = ? WHERE emp_no = ? AND to_date = '9999-01-01'`,
          [dept_no, emp_no]
        );
      } else {
        // Insert a new record if none exists
        await connection.execute(
          `INSERT INTO dept_emp (emp_no, dept_no, from_date, to_date)
           VALUES (?, ?, CURDATE(), '9999-01-01')`,
          [emp_no, dept_no]
        );
      }
    }

    // 3) Update or Insert into `titles` if `title` is provided
    if (title) {
      const [existingTitle] = await connection.execute(
        `SELECT * FROM titles WHERE emp_no = ? AND to_date = '9999-01-01'`,
        [emp_no]
      );
      if (existingTitle.length > 0) {
        await connection.execute(
          `UPDATE titles SET title = ? WHERE emp_no = ? AND to_date = '9999-01-01'`,
          [title, emp_no]
        );
      } else {
        await connection.execute(
          `INSERT INTO titles (emp_no, title, from_date, to_date)
           VALUES (?, ?, CURDATE(), '9999-01-01')`,
          [emp_no, title]
        );
      }
    }

    // 4) Update or Insert into `salaries` if `salary` is provided
    if (salary) {
      const [existingSalary] = await connection.execute(
        `SELECT * FROM salaries WHERE emp_no = ? AND to_date = '9999-01-01'`,
        [emp_no]
      );
      if (existingSalary.length > 0) {
        await connection.execute(
          `UPDATE salaries SET salary = ? WHERE emp_no = ? AND to_date = '9999-01-01'`,
          [salary, emp_no]
        );
      } else {
        await connection.execute(
          `INSERT INTO salaries (emp_no, salary, from_date, to_date)
           VALUES (?, ?, CURDATE(), '9999-01-01')`,
          [emp_no, salary]
        );
      }
    }

    return {
      statusCode: 200,
      body: JSON.stringify({ message: 'Employee updated successfully', emp_no })
    };
  } catch (error) {
    console.error('Error updating employee:', error);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: 'Failed to update employee' })
    };
  } finally {
    if (connection) {
      await connection.end();
    }
  }
};