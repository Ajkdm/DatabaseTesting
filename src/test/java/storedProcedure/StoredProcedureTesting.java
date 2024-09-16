package storedProcedure;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import org.apache.commons.lang3.StringUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class StoredProcedureTesting
{
	Connection con=null;
	Statement st=null;
	ResultSet rs;
	CallableStatement cStmt;
	ResultSet rs1;
	ResultSet rs2;
	
	@BeforeClass
	public void setup() throws SQLException 
	{
		con=DriverManager.getConnection("jdbc:mysql://localhost:3306/classicmodels","root","root");
	}
	
	@AfterClass
	public void tearDown() throws SQLException 
	{
		con.close();
	}
	
	@Test(priority=1)
	public void test_StoredPricedureExist() throws SQLException 
	{
		st=con.createStatement();
		rs=st.executeQuery("show procedure status where Name='SelectAllCustomers'");
		rs.next();
		
		Assert.assertEquals(rs.getString("Name"), "SelectAllCustomers");
	}
	
	
	@Test(priority=2)
	public void selectAllCustomers() throws SQLException 
	{
		cStmt=con.prepareCall("{call SelectAllCustomers()}");
		rs1=cStmt.executeQuery();
		
		Statement stmt=con.createStatement();
		rs2=stmt.executeQuery("select * from customers");
		Assert.assertEquals(compareResultSets(rs1, rs2),true);
	}
	
	@Test(priority=3)
	public void selectAllCustomersByCity() throws SQLException 
	{
		cStmt=con.prepareCall("{call SelectAllCustomersByCity(?)}");
		cStmt.setString(1, "Singapore");
		rs1=cStmt.executeQuery();
		
		Statement stmt=con.createStatement();
		rs2=stmt.executeQuery("Select * from customers where city='Singapore'");
		Assert.assertEquals(compareResultSets(rs1, rs2),true);
	}
	
	@Test(priority=4)
	public void selectAllCustomersByCityAndPin() throws SQLException 
	{
		cStmt=con.prepareCall("{call SelectAllCustomersByCityAndPcode(?,?)}");
		cStmt.setString(1, "Singapore");
		cStmt.setString(2, "079903");
		rs1=cStmt.executeQuery();
		
		Statement stmt=con.createStatement();
		rs2=stmt.executeQuery("Select * from customers where city='Singapore' and postalcode='079903'");
		//rs2=stmt.executeQuery("select * from offices");
		
		Assert.assertEquals(compareResultSets(rs1, rs2),true);
	}
	@Test(priority=5)
	public void tes_get_order_by_cust() throws SQLException 
	{
		cStmt=con.prepareCall("{call get_order_by_cast(?,?,?,?,?)}");
		cStmt.setInt(1, 141);
		
		cStmt.registerOutParameter(2, Types.INTEGER);
		cStmt.registerOutParameter(3, Types.INTEGER);
		cStmt.registerOutParameter(4, Types.INTEGER);
		cStmt.registerOutParameter(5, Types.INTEGER);
		
		cStmt.executeQuery();
		
		int shipped=cStmt.getInt(2);
		int cancelled=cStmt.getInt(3);
		int resolved=cStmt.getInt(4);
		int disputed=cStmt.getInt(5);
		
		System.out.println(shipped+" "+ cancelled+" "+ resolved+" "+ disputed);
		
		Statement stmt=con.createStatement();
		rs=stmt.executeQuery("select\r\n"
				+ "(select count(*) as 'shipped' from orders where customerNumber='141' and status ='Shipped') as Shipped,\r\n"
				+ "(select count(*) as 'cancelled' from orders where customerNumber='141' and status ='Cancelled') as Cancelled,\r\n"
				+ "(select count(*) as 'resolved' from orders where customerNumber='141' and status ='Resolved') as Resolved,\r\n"
				+ "(select count(*) as 'disputed' from orders where customerNumber='141' and status ='Disputed') as Disputed");
		
		rs.next();
		
		int exp_shipped=rs.getInt("shipped");
		int exp_cancelled=rs.getInt("cancelled");
		int exp_resolved=rs.getInt("resolved");
		int exp_disputed=rs.getInt("disputed");
		
		if(shipped==exp_shipped && cancelled==exp_cancelled && resolved==exp_resolved && disputed==exp_disputed)
			Assert.assertTrue(true);
		else
			Assert.assertFalse(false);
		
	}
	
	@Test(priority=6)
	public void test_GetCustomerShipping() throws SQLException 
	{
		cStmt=con.prepareCall("call GetCustomerShiiping(?,?)");
		cStmt.setInt(1, 121);
		cStmt.registerOutParameter(2, Types.VARCHAR);
		
		cStmt.executeQuery();
		
		String shippingTime=cStmt.getString(2);
		
		Statement stmt=con.createStatement();
		rs=stmt.executeQuery("select country,CASE when country='USA' then '2-day Shipping' when country='Canada' then '3-day Shipping' else '5-day Shipping' end as ShippingTime from customers where customerNumber='112'");
		
		rs.next();
		
		String exp_ShippingTime=rs.getString("ShippingTime");
		
		Assert.assertEquals(shippingTime,exp_ShippingTime);
	}
	
	public boolean compareResultSets(ResultSet result1,ResultSet result2) throws SQLException 
	{
		while(result1.next()) 
		{
			result2.next();
			int count=result1.getMetaData().getColumnCount();
			for(int i=1;i<=count;i++) 
			{
				if(!StringUtils.equals(result1.getString(i), result2.getString(i))) 
				{
					return false;
				}
			}
		} 
		return true;
	}
}
