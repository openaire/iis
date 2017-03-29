package helloworld;

/**
 * This class is here only to make the Sonar happy, since when the project
 * does not contain any Java code, Sonar complains. If currently there is some
 * other code in the project, feel free to delete this class along with its
 * package.
 * @author Mateusz Kobos
 *
 */
public final class HelloWorld {

    private HelloWorld() {}
    
	public static void main(String[] args){
		System.out.println("Hello world!");
	}
	
}
