CREATE TABLE field_demos.cross_sell_cyl.ex2_user_silver
AS SELECT
  cast(id as int), 
  sha1(email) as email, 
  to_timestamp(creation_date, "MM-dd-yyyy HH:mm:ss") as creation_date, 
  to_timestamp(last_activity_date, "MM-dd-yyyy HH:mm:ss") as last_activity_date, 
  firstname, 
  lastname, 
  address, 
  city, 
  last_ip, 
  postcode
from field_demos.cross_sell_cyl.ex2_users_bronze