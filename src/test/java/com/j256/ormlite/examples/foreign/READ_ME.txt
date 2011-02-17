This shows how to persist a Order class which has a foreign Account object as one of its fields.

ORMLite supports what it calls "foreign" objects.  These are fields that are other objects stored in the database.
For example, if you have Account and Order objects in your database, the Order objects may have an associated
Account so would have an foreign Account field:

	public class Order {
		
		@DatabaseField(generatedId = true)
		private int id;
		
		@DatabaseField(foreign = true)
		private Account account;
		
		...
	}
 
See the documentation for more information about foreign objects:

	http://ormlite.com/docs/foreign

This example depends on the H2 database which is a native Java SQL implementation.  You can download
the latest jar from the website: http://www.h2database.com/html/download.html
