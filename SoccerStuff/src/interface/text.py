'''
Created on Jan 10, 2017

@author: Bryan Lu
'''

def main():
    while True:
        res_main = input("Welcome to this soccer database. What would you like to do? \n"
              "1. Search \n"
              "2. Simulate \n"
              "3. Exit \n"
        )
        if res_main == '1':
            res_search = input("Are you searching for a (1) player or a (2) team? ")
            if res_search == '1':
                pass
            elif res_search == '2':
                pass
            else:
                print('Input not recognized.')
                
        elif res_main == '2':
            pass
        
        elif res_main == '3':
            print("Goodbye.")
            break
        
        else:
            print('Input not recognized.')

if __name__ == '__main__':
    main() 