#Download dataset from https://data.cityofnewyork.us/Public-Safety/NYPD-Motor-Vehicle-Collisions/h9gi-nx95/data

#Load Data in pig relation:
collision = load '/home/acadgild/NYSE/*' using PigStorage(',');

#limit the data set to 500 records
collision_limited = limit collision 500;

#dump the records for  validation:
dump collision_limited;

#to know the header fields execute below query
collision_header = limit collision_limited 1;

#Validate header fields
dump collision_header;

#segreagate useful data from the collision dataset
collision_useful = foreach collision generate $0 as date,$2 as borough,$3 as zipcode,TRIM($8) as location,$11+$13+$15+$17 as injured, TRIM ($19) as reason; 

dump collision_useful;

describe collision_useful;

#Segraget data according to reason of accident
colision_reason = foreach collision_useful generate reason,borough,injured;

#grop the data according to reason:reason for collision in a one bag
collision_grp = group colision_reason by reason;
collision_borough = group colision_reason by borough;
#in the above relation data is converted into bag and every bag will contain whole lot of records.
dump collision_grp;

#objectives:
# 1. What Kind of collision causes most injuries in the new york?
#using SUM() aggregation
#use relation on which reason is avaialble

total_injured_reason = foreach collision_grp generate group,SUM(colision_reason.injured);

# 2. Collision occured in different borough use COUNT() for aggregation


total_injured_borough = foreach collision_borough generate group,SUM(colision_reason.borough);

#Use Nested Foreach on NYC collision data
#1. total number of collision per borough
#2. Top 2 reason for collision

#clean up and process data before using nested foreach

collision_injured = FOREACH collision_useful generate reason,borough,location,injured;

#calculate number of collision per borough per reason

collision_group = group collision_injured by (borough,reason);

#Total Number of collision per borough per reason:count the no. of collision

collision_total_raw = foreach collision_group generate group.borough,group.reason,COUNT(collision_injured) as total;

#eliminate record where either borough or reason is missing or invalid use filter command for clean up
collision_total = FILTER collision_total_raw by borough is not null and reason is not null;

#Total number of collision per borough :group by borough

collision_total_group = group collision_total by borough;

#Total Number of collision per borough top reason for collision per borough:use nested for each

collision_stats = foreach collision_total_group {
total_collision = SUM(collision_total.total);
generate group,total_collision;
}

#generate in descending order of  total_per_reason
collision_stats2 = foreach collision_total_group {
total_collision = SUM(collision_total.total);
sorted_collision = order collision_total by total desc;
generate flatten (sorted_collision),total_collision;
}

#highest 2 records

collision_stats2 = foreach collision_total_group {
total_collision = SUM(collision_total.total);
sorted_collision = order collision_total by total desc;
highest_num_collision = limit sorted_collision 2;
generate flatten (highest_num_collision),total_collision;
}



