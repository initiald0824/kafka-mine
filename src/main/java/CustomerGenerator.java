/**
 * 自定义Customer生成器
 */
public class CustomerGenerator implements Generator<Customer> {
    private static int counter=1;
    private final int id=counter++;

    public static Customer getNext() {
        return new CustomerGenerator().next();
    }

    public Customer next() {

        return new Customer(id,String.format("customer(%s)",id));
    }
}
