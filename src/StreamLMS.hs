{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE FlexibleInstances #-}
{-# OPTIONS_GHC -Wno-orphans #-}
module StreamLMS where

import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as BC
import Data.ByteString (ByteString)
import Control.Monad
import Data.List
import Data.Maybe
import Language.Haskell.TH
import Language.Haskell.TH.Syntax
import Prelude hiding (Applicative(..))
import Instances.TH.Lift ()
--import Data.FileEmbed
import Data.Functor.Identity
import Control.Applicative (liftA2)
import qualified Prelude as P
import qualified Data.ByteString.Streaming.Char8 as Q
import Control.Monad.Trans.Resource
import System.IO
import Debug.Trace
import Data.Functor.Of

class Ops r where
  eq :: Eq a => r a -> r a -> r Bool
  leq :: Eq a => r (IO a) -> r (IO a) -> r (IO Bool)
  lneq :: Eq a => r (IO a) -> r (IO a) -> r (IO Bool)
  neq :: Eq a => r a -> r a -> r Bool
  bc_split :: r Char -> r ByteString -> r [ByteString]
  tail_dropwhile :: Char -> r (BS ()) -> r (BS ())
  take_while :: Char -> r (BS ()) -> r (BS ())

  _case_of :: r (Of a b) -> r (a -> b -> c) -> r c

  _if :: r Bool -> r a -> r a -> r a
  _fix :: (r a -> r a) -> r a
  _lam :: (r a -> r b) -> r (a -> b)

  _bind :: Monad m => r (m a) -> (r (a -> m b)) -> r (m b)
  _print :: Show a => r a -> r Res
  _putStr :: r (BS ())  -> r (IO ())
  (>>>) :: Monoid m => r m -> r m -> r m
  _empty :: Monoid m => r m
  _pure :: P.Applicative f => r a -> r (f a)

  -- Scanner interface
  _newScanner :: FilePath -> r (IO Scanner)
  _hasNext :: r Scanner -> r (IO (Of Bool Scanner))
  _nextLine :: r Scanner -> (r (BS ()), r Scanner)

  liftBS :: r ByteString -> r (BS ())
  runBS :: r (BS ()) -> r (IO ByteString)

  pure :: Lift a => a -> r a
  (<*>) :: r (a -> b) -> r a -> r b

infixl 4 <*>

newtype Code a = Code (Q (TExp a))

instance Ops Code where
  eq (Code e1) (Code e2) = Code [|| $$e1 == $$e2 ||]
  neq (Code e1) (Code e2) = Code [|| $$e1 /= $$e2 ||]
  leq (Code e1) (Code e2) = Code [|| (==) <$> $$e1 P.<*> $$e2 ||]
  lneq (Code e1) (Code e2) = Code [|| (/=) <$> $$e1 P.<*> $$e2 ||]
  bc_split (Code e1) (Code e2) = Code [|| BC.split $$e1 $$e2 ||]
  _if (Code a) (Code b) (Code c) = Code [|| if $$a then $$b else $$c ||]
  (>>>) (Code a) (Code b) = Code [|| $$a <> $$b ||]
  _bind (Code a) (Code b) = Code [|| $$a >>= $$b ||]
  _empty = Code [|| mempty ||]
  _pure (Code p) = Code [|| P.pure $$p ||]

  _print (Code a) = Code [|| print $$a ||]
  _putStr (Code a) = Code [|| Q.putStr $$a ||]

  _case_of (Code a) (Code b) = Code [|| case $$a of
                                                  (v1 :> v2) -> $$b v1 v2 ||]

  tail_dropwhile c (Code b) = Code [|| Q.drop 1 (Q.dropWhile (/= c) $$b) ||]
  take_while c (Code b) = Code [|| Q.takeWhile (/= c) $$b ||]
  _fix f = Code [|| fix (\a -> $$(runCode $ f (Code [||a||]))) ||]

  _lam f = Code $ [|| \a ->  $$(runCode $ f (Code [|| a ||]))  ||]

  liftBS (Code b) = Code $ [|| Q.fromStrict $$(b) ||]
  runBS (Code b)  = Code $ [|| Q.toStrict_ $$b ||]


  _newScanner fp = Code [|| newScanner fp ||]
  _hasNext s = Code [|| hasNext $$(runCode s) ||]
  _nextLine = _nextLineCode



  pure = Code . unsafeTExpCoerce . lift
  (Code f) <*> (Code a) = Code [|| $$f $$a ||]


instance Ops Identity where
  eq = liftA2 (==)
  leq = liftA2 (liftA2 (==))
  lneq = liftA2 (liftA2 (==))

  neq = liftA2 (/=)
  bc_split = liftA2 (BC.split)
  tail_dropwhile c = fmap (Q.drop 1 . (Q.dropWhile (/= c)))
  take_while c = fmap (Q.takeWhile (/= c))
  _print = fmap print
  _putStr = fmap (Q.putStr)
  _empty = Identity mempty
  _if (Identity b) (Identity c1) (Identity c2) = Identity (if b then c1 else c2)

  _fix = fix
  _lam f = Identity (\a -> runIdentity (f (Identity a)))
  (>>>) = liftA2 mappend
  _bind = liftA2 (>>=)
  _pure = fmap (P.pure)
  _newScanner fp = Identity (newScanner fp)
  _hasNext = fmap hasNext
  _nextLine = nextLine

  liftBS b = Q.fromStrict <$> b
  runBS b  = Q.toStrict_ <$> b

  pure = Identity
  (<*>) (Identity a1) (Identity a2) = Identity (a1 a2)


list_eq :: (Ops r, Eq a) => [r (IO a)] -> [r (IO a)] -> r (IO Bool)
list_eq [] [] = _pure (pure True)
list_eq (v:vs) (v1:v1s) =
  (leq v v1) `_bind` (_lam $ \b ->
    _if b  (list_eq vs v1s) (_pure (pure False)))
list_eq _ _ = _pure (pure False)

_when :: (Monoid m, Ops r) => r (IO Bool) -> r (IO m) -> r (IO m)
_when cond act = cond `_bind` (_lam $ \b -> _if b act _empty)

_whenOf :: (Monoid m, Ops r) => r (IO (Of Bool t)) -> r (t -> IO m) -> r (IO m)
_whenOf cond act = cond `_bind` (_lam $ \b
                      -> _case_of b (_lam $ \b' -> _lam $ \t
                                        -> _if b' (act <*> t) _empty))

_whenA :: (P.Applicative f, Ops r) => r Bool -> r (f ()) -> r (f ())
_whenA cond act = _if cond act (_pure (pure ()))

runCode :: Code a -> Q (TExp a)
runCode (Code a) = a



type Fields r = [r (BS ())]
type Schema = [ByteString]
type Table = FilePath

type Res = IO ()

data Record r = Record { fields :: Fields r, schema :: Schema }

getField :: ByteString -> Record r -> r (BS ())
getField field (Record fs sch) =
  let i = fromJust (elemIndex field sch)
  in  (fs !! i)

getFields :: [ByteString] -> Record r -> [r (BS ())]
getFields fs r = map (flip getField r) fs


data Operator = Scan FilePath Schema | Project Schema Schema Operator
              | Filter Predicate Operator | Join Operator Operator deriving Show

query, query2 :: Operator
query = Project ["age"] ["age"] (Filter (Eq (Value "john") (Field "name")) (Scan "data/test.csv" ["name", "age"]))

query2 = Project ["name"] ["name"] (Filter (Eq (Value "34") (Field "age")) (Scan "data/test.csv" ["name", "age"]))

queryJoin :: Operator
queryJoin = Join (Scan "data/test.csv" ["name", "age"]) (Scan "data/test1.csv" ["name", "weight"])

queryP :: Operator
queryP = Project ["name"] ["name"] (Scan "data/test.csv" ["name", "age"])

data Predicate = Eq Ref Ref | Ne Ref Ref deriving Show

data Ref = Field ByteString | Value ByteString deriving Show

type QTExp a = Code a

fix :: (a -> a) -> a
fix f = let x = f x in x

data Scanner = Scanner (BS ())

type BS a = Q.ByteString IO a

newScanner :: FilePath -> IO Scanner
newScanner fp =
  openFile fp ReadMode >>= \h -> return (Scanner (Q.fromHandle h))

nextLine :: Identity Scanner -> (Identity (BS ()), Identity Scanner)
nextLine (Identity (Scanner bs)) =
  (Identity $ Q.takeWhile (/= '\n') bs,
   Identity $ Scanner (Q.drop 1 (Q.dropWhile (/= '\n') bs)))

-- As |span| is not stage aware, it is more dynamic an necessary. Splitting
-- the implementation up means that we can skip over entire rows if
-- necessary in the generated code.
_nextLineCode :: Code Scanner -> (Code (BS ()), Code Scanner)
_nextLineCode scanner =
  let fs = Code [|| let (Scanner s) = $$(runCode scanner) in Q.takeWhile (/= '\n') s ||]
      ts = Code [|| let (Scanner s) = $$(runCode scanner) in Scanner (Q.drop 1 (Q.dropWhile (/= '\n') s)) ||]
  in (fs, ts)



hasNext :: Scanner -> IO (Of Bool Scanner)
hasNext (Scanner bs) = (\(b :> s) -> (not b :> Scanner s)) <$> Q.testNull bs

instance Semigroup m => Semigroup (ResourceT IO m) where
  (<>) a1 a2 = do
    (<>) <$> a1 P.<*> a2

instance Monoid m => Monoid (ResourceT IO m) where
  mempty = P.pure mempty

while ::
  (Ops r, Monoid m) =>
  r (t -> IO (Of Bool t)) -> r ((t -> IO m) -> t -> IO m) -> r (t -> IO m)
while k b = _fix (\r -> _lam $ \rs -> _whenOf (k <*> rs) (_lam $ \rs -> b <*> r <*> rs))

whenM :: Monoid m => Bool -> m -> m
whenM b act = if b then act else mempty

processCSV :: forall m r . (Monoid m, Ops r) => Schema -> FilePath -> (Record r -> r (IO m)) -> r (IO m)
processCSV ss f yld =
  _newScanner f `_bind` rows ss
  where
    rows :: Schema -> r (Scanner -> (IO m))
    rows sch = do
      while (_lam _hasNext)
            (_lam $ \r -> _lam $ \rs ->
              (let (hs, ts) = _nextLine rs
              in yld (Record (parseRow sch hs) sch) >>> (r <*> ts))
              )

    -- We can't use the standard |BC.split| function here because
    -- we we statically know how far we will unroll. The result is then
    -- more static as we can do things like drop certain fields if we
    -- perform a projection.
    parseRow :: Schema -> r (BS ()) -> [r (BS ())]
    parseRow [] _ = []
    parseRow [_] b =
      [take_while '\n' b]
    parseRow (_:ss') b =
      let new = tail_dropwhile ',' b
          rs = parseRow ss' new
      in (take_while ',' b  : rs)

printFields :: Ops r => Fields r -> r (IO ())
printFields [] = _empty
printFields [x] = _putStr x >>> _putStr (liftBS (pure "\n"))
printFields (x:xs) =
  _putStr x >>> _putStr (liftBS $ pure ",") >>> printFields xs

evalPred :: Ops r => Predicate -> Record r -> r (IO Bool)
evalPred  predicate rec =
  case predicate of
    Eq a b -> leq (evalRef a rec) (evalRef b rec)
    Ne a b -> lneq (evalRef a rec) (evalRef b rec)

evalRef :: Ops r => Ref -> Record r -> r (IO ByteString)
evalRef (Value a) _ = _pure (pure a)
evalRef (Field name) r = runBS $ getField name r


restrict :: Ops r => Record r -> Schema -> Schema -> Record r
restrict r newSchema parentSchema =
  Record (map (flip getField r) parentSchema) newSchema


execOp :: (Monoid m, Ops r) => Operator -> (Record r -> r (IO m)) -> r (IO m)
execOp op yld =
  case op of
    Scan file sch ->
      processCSV sch file yld
    Filter predicate parent -> execOp parent
      (\rec -> _when (evalPred predicate rec) (yld rec) )
    Project newSchema parentSchema parent ->
      execOp parent (\rec -> yld (restrict rec newSchema parentSchema ))
    Join left right ->
      execOp left (\rec -> execOp right (\rec' ->
        let keys = schema rec `intersect` schema rec'
        in _when (list_eq (map runBS $ getFields keys rec) (map runBS $ getFields keys rec'))
               (yld (Record (fields rec ++ fields rec')
                       (schema rec ++ schema rec')))))

runQuery :: Operator -> Q (TExp Res)
runQuery q = [|| $$(runCode $ execOp q (printFields . fields)) ||]

runQueryL :: Operator -> Q (TExp (IO [Record Identity]))
runQueryL q = [|| $$(runCode $ execOp q (\r -> Code [|| return (return $$(runCode $ spill r)) ||])) ||]

spill :: Record Code -> Code (Record Identity)
spill (Record rs ss) = Code [|| Record $$(runCode $ spill2 rs) ss ||]

spill2 :: [Code a] -> Code [Identity a]
spill2 [] = Code [|| [] ||]
spill2 (x:xs) = Code [|| (Identity $$(runCode x)) : $$(runCode $ spill2 xs) ||]

runQueryUnstaged :: Operator -> Res
runQueryUnstaged q =  runIdentity (execOp q (printFields . fields))

runQueryUnstagedL :: Operator -> IO [Record Identity]
runQueryUnstagedL q =  runIdentity (execOp q (return . return . return))

test :: IO ()
test = do
--  processCSV "data/test.csv" (print . getField "name")
  expr <- runQ $ unTypeQ  $ runCode $ execOp query (printFields . fields)
--  expr <- runQ $ unTypeQ  $ power
  putStrLn $ pprint expr


bsToExp :: B.ByteString -> Q Exp
bsToExp bs = do
    helper <- [| stringToBs |]
    let chars = BC.unpack . BC.tail . (BC.dropWhile (/= '\n')) $ bs
    return $! AppE helper $! LitE $! StringL chars

stringToBs :: String -> B.ByteString
stringToBs = BC.pack

