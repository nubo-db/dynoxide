/**
 * The bundled dynoxide Worker entry. It exports nothing: construct it as a
 * module Worker (see the package README), it is not meant to be imported for
 * values. This declaration just gives the `./worker` subpath a type so a strict
 * TypeScript consumer that imports it does not fall back to implicit any.
 */
export {};
