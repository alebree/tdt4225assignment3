def query5(self):
    rows = self.db.Activity.aggregate( [
  {
    '$group':{
      "_id": {
        "user_id": "$user_id",  
        "transportation_mode": "$transportation_mode", 
        "start_date_time": "$start_date_time",
        "end_date_time": "$end_date_time"
        },           
      "count": {'$sum':1}
      }
  },
  {
    '$match':{
      "count": { '$gt': 1 }
    }
  },
  {
    '$project': {
      "user_id":"_id.user_id", 
      "transportation_mode":"_id.transportation_mode",  
      "start_date_time":"_id.start_date_time",
      "end_date_time":"_id.end_date_time",
      "count": 1,
    }
  }
])