package Controller;



import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.mvc.Controller;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author Zhuang Zhuo <zhuo.z@husky.neu.edu>
 */
public class MovieController implements Controller{

    public ModelAndView handleRequest(HttpServletRequest request, HttpServletResponse response) throws Exception {
       
                   String type = request.getParameter("type");
                   
                   ModelAndView mv = new ModelAndView();
                   if(type.equals("map1")){
                mv.setViewName("cancela");
                   }
                 else{
                         if(type.equals("map2")){
                           mv.setViewName("delay");
                         }else
                         {
                        mv.setViewName("top30delay");
                         }
                         }
        
        return mv;     
        
    }


    
}

