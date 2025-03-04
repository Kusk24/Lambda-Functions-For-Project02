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

    // Check if we have a path parameter, e.g., /employees/{emp_no}
    const emp_no = event.pathParameters?.emp_no;

    let query;
    let params = [];

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

    const [rows] = await connection.execute(query, params);

    return {
      statusCode: 200,
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(rows)
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
  }
};