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

// Promisify memcached delete method for easier async/await usage
const delAsync = util.promisify(memcached.del).bind(memcached);

exports.handler = async (event) => {
  let connection;
  try {
    connection = await mysql.createConnection(dbConfig);

    // Parse JSON body
    // Example body:
    // {
    //   "emp_no": 999999
    // }
    const body = JSON.parse(event.body || '{}');
    const { emp_no } = body;

    if (!emp_no) {
      return {
        statusCode: 400,
        body: JSON.stringify({ error: 'Missing emp_no for delete' })
      };
    }

    // 1) Delete from referencing tables first
    await connection.execute(`DELETE FROM dept_emp WHERE emp_no = ?`, [emp_no]);
    await connection.execute(`DELETE FROM titles WHERE emp_no = ?`, [emp_no]);
    await connection.execute(`DELETE FROM salaries WHERE emp_no = ?`, [emp_no]);
    await connection.execute(`DELETE FROM dept_manager WHERE emp_no = ?`, [emp_no]);

    // 2) Then delete from employees
    const [deleteResult] = await connection.execute(`DELETE FROM employees WHERE emp_no = ?`, [emp_no]);

    // Optionally check deleteResult.affectedRows to verify deletion if needed

    // Invalidate cache entries affected by the delete.
    // Flush the specific employee's cache and the top employees cache.
    try {
      const empCacheKey = `employee_${emp_no}`;
      await delAsync(empCacheKey);
      console.log(`Flushed cache for key: ${empCacheKey}`);
      await delAsync('top_employees');
      console.log('Flushed cache for key: top_employees');
    } catch (cacheError) {
      console.error('Memcache delete error:', cacheError);
    }

    return {
      statusCode: 200,
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        message: 'Employee deleted successfully',
        emp_no
      })
    };
  } catch (error) {
    console.error('Error deleting employee:', error);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: 'Failed to delete employee' })
    };
  } finally {
    if (connection) {
      await connection.end();
    }
  }
};
