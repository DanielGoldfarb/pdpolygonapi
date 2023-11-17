import requests

class UnboundTest():

    def __init__(self):
        pass

    def print_var(self,var):
        print(var)

    def mod_var(self,var):
        var = 'modified'
        print('mod_var:',var)

    def outer_function(self,a='1',b='2',c='3'):

        req = ('This is a test'+
             ' of x')
        y = b + c
       
        def inner_function():
            print('a,b,c=',a,b,c)
            print('req,y=',req,y)
            self.print_var(req)
            self.mod_var(req)
            self.print_var(req)
            rqq = req
            req = rqq
            #req = 'here we go again'
            #print('a,b,c=',a,b,c)
            #print('req,y=',req,y)
    
        if not True:
            pass
        else:
            inner_function()

u = UnboundTest()

u.outer_function('3','4','5')

