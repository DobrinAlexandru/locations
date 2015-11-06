function test() {
  var timerStart = Date.now();
  var i = 0;
  var c = 1;
  for (i = 0; i < 200000000; i ++) {
    var a  = 2;
    var b = a + 5;
    c = a + b;
    if (a < b) {
      c = c - a;
    } else {
      c = a;
    }
  }
  console.log("time spent: " + (Date.now() - timerStart));
}

test();