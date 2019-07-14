function func1(){
    var int_array = Java.type("int[]");
    var a = new int_array(4);
    a[0] = 3;
    a[1] = 32;
    a[2] = 1;
    a[3] = 4;

    var total = 0;
    for (i = 0; i< a.length; i++){
        total += a[i]
    }
    return total;
}
