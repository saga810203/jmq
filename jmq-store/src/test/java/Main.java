
public class Main {

	public static void main(String[] args)throws Exception {
		short l = Short.MAX_VALUE;
		System.out.println(l);
		System.out.println(Long.toHexString(l));
		++l;
		System.out.println(l);
		System.out.println(Long.toHexString(l));
	}
}
