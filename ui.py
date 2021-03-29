import sparkprac

def printMenu():
    print("--------------MENU--------------")
    print("Please type the number corresponding to an option from the menu below to see the printed results")
    print()
    print("1. Search by user ID to get the number of movies+genres he/she watched") #implemented as two separate functions (might just be another option tbh)
    print("2. Given a list of users get a list of movies + genres they watched")
    print("3. Search movie by ID or Title to get the Average Rating and the Number of users that have watched it ") #implemented as two separate functions
    print("4. Find all movies corresponding to a genre or a list of genres") #implemented as two separate functions
    print("5. Search for movies by year")
    print("6. List the Top n movies with highest rating, ordered by rating")
    print("7. List the Top n movies with the highest number of watches, ordered by number of watches")
    print("8. Find the favourite genre of a given user")
    print("9. Compare...")
    print("10. To exit the program")
    print("Enter show menu to see the menu again")
    

def main():
    printMenu()
    while (True):
        # printMenu()
        userInput = input("Enter a number from 1-9 that matches the option you want to explore in the menu shown above\n")

        if(userInput == "1"):
            userInput = input("Please enter the user ID you want to search\n")
            print(sparkprac.searchUserId(userInput))
        elif(userInput == "2"):
            userInput = input("Please enter a list of user IDs (comma separated)\n")
            userList = userInput.split(",")
            print(sparkprac.searchUserListMovies(userList))
        elif(userInput == "3"):
            print("Enter 1 if you're searching by movie ID")
            print("Enter 2 if you're searching by Title")
            userInput = input("Enter 1 or 2\n")
            if(userInput == "1"):
                userInput = input("Please enter movie ID\n")
                print(sparkprac.searchMovieById(userInput))
            elif(userInput == "2"):
                userInput = input("please enter movie Title\n")
                print(sparkprac.searchMovieByTitle(userInput))
        elif(userInput == "4"):
            print("Enter 1 if you're inputting a genre")
            print("Enter 2 if you're inputting a list of genres")
            userInput = input("Enter 1 or 2\n")
            if(userInput == "1"):
                userInput = input("Enter a genre\n")
                print(sparkprac.searchMovieByGenre(userInput, 5)) #limit is temp
            elif(userInput == "2"):   #might want to consider returning distinct movies
                userInput = input("Enter comma separated list of genres\n")
                genreList = userInput.split(",")
                print(sparkprac.searchByMovieGenreList(genreList, 5)) #limit is temp
        elif(userInput == "5"):
            userInput = input("Please enter the year you want to search by\n")
            print(sparkprac.searchMovieByYear(userInput, 5)) #limit is temp
        elif(userInput == "6"):
            userInput = input("Please enter number of Top N moves you want returned\n")
            print(sparkprac.nTopMovieRating(int(userInput)))
        elif(userInput == "7"):
            userInput = input("Please enter number of Top movies watched you want returned\n")
            print(sparkprac.nTopMovieWatches(int(userInput)))
        elif(userInput == "8"):
            userInput = input("Please enter user id\n")
            print("User: " + userInput + " likes " + sparkprac.findFavGenre(userInput)[0] + " movies")
        elif(userInput == "9"):
            comparableList = []
            userInput = input("Please enter user id 1\n")
            a = "User: " + userInput + " likes " + sparkprac.findFavGenre(userInput)[0] + " movies"
            comparableList.append(a)
            userInput = input("Please enter user id 2\n")
            b = "User: " + userInput + " likes " + sparkprac.findFavGenre(userInput)[0] + " movies"
            comparableList.append(b)
            for i in comparableList:
                print(i)
        elif(userInput == "show menu"):
            printMenu()
        elif(userInput == "10"):
            print("Thank you for using our program, see you next time!")
            exit()

main()
