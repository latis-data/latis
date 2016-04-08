attributes {
    samples{
        myTime{
            string units "yyyy/MM/dd";
            string alias "time";
            string length "10";
        }
        unknown{
            myInt{
                string missing_value "3";
                string long_name "myInteger";
                string units "N";
            }
            myReal{
                string units "m/s^2";
                string long_name "myReal";
            }
            myText{
                string units "letters";
                string missing_value "D";
                string long_name "text";
            }
        }
    }
}