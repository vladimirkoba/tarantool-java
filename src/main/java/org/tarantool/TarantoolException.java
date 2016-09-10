package org.tarantool;

/**
 * A remote server error with error code and message.
 * 
 * @author dgreen
 * @version $Id: $
 */
public class TarantoolException extends RuntimeException {

	private static final long serialVersionUID = 1L;
	long code;

	/**
	 * <p>
	 * Getter for the field <code>code</code>.
	 * </p>
	 * 
	 * @return a int.
	 */
	public long getCode() {
		return code;
	}

	/**
	 * <p>
	 * Constructor for TarantoolException.
	 * </p>
	 * 
	 * @param code
	 *            a int.
	 * @param message
	 *            a {@link java.lang.String} object.
	 * @param cause
	 *            a {@link java.lang.Throwable} object.
	 */
	public TarantoolException(long code, String message, Throwable cause) {
		super(message, cause);
		this.code = code;

	}

	/**
	 * <p>
	 * Constructor for TarantoolException.
	 * </p>
	 * 
	 * @param code
	 *            a int.
	 * @param message
	 *            a {@link java.lang.String} object.
	 */
	public TarantoolException(long code, String message) {
		super(message);
		this.code = code;

	}

}
