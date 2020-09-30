This is similar to the "foreign" example, but this shows how to use "foreign collections".

Foreign collections are collections of objects in other tables that match the current
object.  For example, if you have Account and Order objects in your database, the Order
objects may have an associated Account so would have an foreign Account field.  With
foreign objects, the Account can have a Collection of orders that match the account.  See
the documentation for more information:

	http://ormlite.com/docs/foreign-collection

This example depends on the H2 database which is a native Java SQL implementation.  You
can download the latest jar from the website:

    http://www.h2database.com/html/download.html

For more examples see: http://ormlite.com/docs/examples
