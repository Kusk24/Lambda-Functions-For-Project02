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
    //   "emp_no": 999999,
    //   "first_name": "John",
    //   "last_name": "Doe",
    //   "gender": "M",
    //   "birth_date": "1980-01-01",
    //   "hire_date": "2020-01-01",
    //   "dept_no": "d001",
    //   "title": "Engineer",
    //   "salary": 60000
    // }
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

    return {
      statusCode: 201,
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        message: 'Employee created successfully',
        emp_no
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