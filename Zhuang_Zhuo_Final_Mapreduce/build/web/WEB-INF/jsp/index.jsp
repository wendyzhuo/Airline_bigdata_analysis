<%@page contentType="text/html" pageEncoding="UTF-8"%>
<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN"
    "http://www.w3.org/TR/html4/loose.dtd">

<html>
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
        <title>Welcome to Spring Web MVC project</title>
    </head>

    <body>
         <h1>Search Movies</h1>
        <form method="post" action="movieController.htm">
              <input type="hidden" name="page" value="2"/>
            <input type = 'radio' name='type' value='map1' />Cancelation<br>
            <input type = 'radio' name='type' value='map2' />Delay time and the amount of flights  of the airlines<br>
            <input type = 'radio' name='type' value='map3' /> top 30 average delay time <br>
            <input type="submit" value="Search Movie"/>
        </form>
    </body>
</html>
