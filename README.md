# datanode_3

Para poder utilizar el código se deben seguir los siguientes pasos:

- Ingrese a las máquinas virtuales:
1. DataNode_1: 10.10.28.17:9000
2. DataNode_2: 10.10.28.17:9000
3. DataNode_3: 10.10.28.17:9000 
4. NameNode: 10.10.28.17:9000

- Se deben activar **todos** los servidores en cada máquina virtual, para ello, en una terminal de las máquinas virtuales correspondientes al DataNode_1, DataNode_2 y DataNode_3 se debe ingresar el siguiente comando:

>`` make dataNode ``

- Para la máquina virtual del NameNode se debe ingresar:

>``make nameNode``

- Finalmente, para probar el programa debemos utilizar otra terminal donde se encuentra la máquina virtual del DataNode_1 donde debemos ingresar lo siguiente:

>``make client``

- Para cargar un libro se debe escoger la opción *"0"*, luego, debemos decidir qué tipo de distribución utilizaremos para guardar los libros en los diferentes DataNodes, la opción 0 es para distribución centralizada y la opción "1" es para la opción distribuida. 

- Finalmente, para que la carga del libro se realice correctamente se debe ingresar el directorio que contiene al libro, por ejemplo, si se tiene un libro llamado "Hola_soy_un_libro.pdf" en el directorio "/books" el input requerido sería "/books/Hola_soy_un_libro.pdf". Después de esto, se debe ingresar el nombre del libro, sin la extensión. Siguiendo nuestro ejemplo se debería ingresar "Hola_soy_un_libro". Así, se registra en un archivo diferente al log del NameNode, qué libros han sido cargados y pueden ser buscados al momento de descargar.

- Para descargar un libro se escoge la opción "1", aquí se presenta una lista con los nombres de todos los libros cargados con anterioridad.
Se debe escoger uno de los nombres que aparecen en la lista e ingresarlo y este es descargado.