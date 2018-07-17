This repo contains a Haskell implementation of

> Rompf, Tiark, and Nada Amin. "Functional pearl: a SQL to C compiler in 500 lines of code." Acm Sigplan Notices 50.9 (2015): 2-9.

The paper describes a simple SQL query compiler which uses generative programming
techniques. The idea is that we first define a definitional interpreter, which
we then stage in order to produce a compiler for a specific known SQL query.
Further to this, the authors use the LMS framework in scala which makes staging
a program as easy as adding some type annotations. The `LMS` module explicitly
implements this idea without any magic that LMS performs to make the transformation
appear transparent.

There are 3 modules of interest:

1 `Interpreter.hs` - A basic naive interpreter
2. `Compiler.hs` - A staged interpreter
3. `LMS.hs` - A definition which can either be specialised to a compiler or an
              interpreter by providing a type annotation.


