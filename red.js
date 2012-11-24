function (values, arg) {
  return [
    values.reduce( function(acc, item) {
      for(var month in item) {
        if(acc[month]) {
          acc[month] = (acc[month] < item[month]) ? item[month] : acc[month];}
        else {
          acc[month] = item[month]; 
        }
      }
      return acc;
      }
    )
  ];
}
