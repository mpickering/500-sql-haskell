{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE ConstraintKinds #-}
module LMS where

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
import System.IO.Unsafe
import qualified Prelude as P
import GHC.TypeLits
import Control.Applicative (liftA3)



class Ops r where
  eq :: Eq a => r a -> r a -> r Bool
  neq :: Eq a => r a -> r a -> r Bool
  bc_split :: r Char -> r ByteString -> r [ByteString]
  tail_dropwhile :: Char -> r ByteString -> r ByteString
  take_while :: Char -> r ByteString -> r ByteString

  _if :: r Bool -> r a -> r a -> r a
  _caseString :: r ByteString -> r a -> r a -> r a
  _fix :: (r a -> r a) -> r a
  _lam :: (r a -> r b) -> r (a -> b)

  _bind :: Monad m => r (m a) -> (r (a -> m b)) -> r (m b)
  _print :: Show a => r a -> r Res
  _putStr :: r ByteString -> r Res
  (>>>) :: Monoid m => r m -> r m -> r m
  _empty :: Monoid m => r m
  _pure :: P.Applicative f => r a -> r (f a)

  -- Scanner interface
  _newScanner :: FilePath -> r (IO Scanner)
  _hasNext :: r Scanner -> r Bool
  _nextLine :: r Scanner -> (r ByteString, r Scanner)

  pure :: Lift a => a -> r a
  (<*>) :: r (a -> b) -> r a -> r b

  _case_record :: r (Record r1)
               -> r ((Fields r1) -> Schema -> c)
               -> r c

  _lup :: r ByteString -> r (Fields r1) -> r Schema -> r (r1 ByteString)

  _intersect :: Eq a => r [a] -> r [a] -> r [a]
  _mkRecord :: r (Fields r1) -> r Schema -> r (Record r1)
  _cons :: r a -> r [a] -> r [a]



infixl 4 <*>

newtype Code a = Code (Q (TExp a))

instance Ops Code where
  eq (Code e1) (Code e2) = Code [|| $$e1 == $$e2 ||]
  neq (Code e1) (Code e2) = Code [|| $$e1 /= $$e2 ||]
  bc_split (Code e1) (Code e2) = Code [|| BC.split $$e1 $$e2 ||]
  _if (Code a) (Code b) (Code c) = Code [|| if $$a then $$b else $$c ||]
  _caseString (Code a) (Code b) (Code c) =
    Code [|| case $$a of
                "" -> $$b
                _  -> $$c ||]
  (>>>) (Code a) (Code b) = Code [|| $$a <> $$b ||]
  _bind (Code a) (Code b) = Code [|| $$a >>= $$b ||]
  _empty = Code [|| mempty ||]
  _pure (Code p) = Code [|| P.pure $$p ||]

  _print (Code a) = Code [|| print $$a ||]
  _putStr (Code a) = Code [|| BC.putStr $$a ||]

  tail_dropwhile c (Code b) = Code [|| BC.tail (BC.dropWhile (/= c) $$b) ||]
  take_while c (Code b) = Code [|| BC.takeWhile (/= c) $$b ||]
  _fix f = Code [|| fix (\a -> $$(runCode $ f (Code [||a||]))) ||]

  _lam f = Code $ [|| \a ->  $$(runCode $ f (Code [|| a ||]))  ||]


  _newScanner fp = Code [|| newScanner fp ||]
  _hasNext s = Code [|| hasNext $$(runCode s) ||]
  _nextLine = _nextLineCode



  pure = Code . unsafeTExpCoerce . lift
  (Code f) <*> (Code a) = Code [|| $$f $$a ||]

  _case_record (Code r) (Code k1) =
    Code [|| case $$r of
              Record rs ss -> $$(k1) rs ss ||]

  _lup (Code l1) (Code l2) (Code l3) = Code [|| lup $$l1 $$l2 $$l3 ||]

  _intersect (Code l1) (Code l2) = Code [|| $$l1 `intersect` $$l2 ||]

  _mkRecord (Code l1) (Code l2) = Code [|| Record $$l1 $$l2 ||]

  _cons (Code l1) (Code l2) = Code [|| $$l1 : $$l2 ||]


instance Ops Identity where
  eq = liftA2 (==)
  neq = liftA2 (/=)
  bc_split = liftA2 (BC.split)
  tail_dropwhile c = fmap (BC.tail . (BC.dropWhile (/= c)))
  take_while c = fmap (BC.takeWhile (/= c))
  _print = fmap print
  _putStr = fmap (BC.putStr)
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

  pure = Identity
  (<*>) (Identity a1) (Identity a2) = Identity (a1 a2)

  _case_record (Identity r) (Identity k) =
    case r of
      Record rs ss -> Identity (k rs ss)

  _lup = liftA3 lup

  _intersect = liftA2 intersect
  _mkRecord = liftA2 Record

  _cons = liftA2 (:)



_when :: (Monoid m, Ops r) => r Bool -> r m -> r m
_when cond act = _if cond act _empty

_whenA :: (P.Applicative f, Ops r) => r Bool -> r (f ()) -> r (f ())
_whenA cond act = _if cond act (_pure (pure ()))

runCode :: Code a -> Q (TExp a)
runCode (Code a) = a



type Fields r = [r ByteString]
type Schema = [ByteString]
type Table = FilePath

type Res = IO ()

data Record r = Record { fields :: Fields r, schema :: Schema }

getField :: Ops r1 => r1 ByteString -> r1 (Record r) -> r1 (r ByteString)
getField field r =
  _case_record r (_lam $ \fs -> _lam $ \sch -> _lup field fs sch)


lup :: ByteString -> Fields r -> Schema -> r ByteString
lup field fs sch =
  let i = fromJust (elemIndex field sch)
  in  (fs !! i)

--getFields :: [ByteString] -> Record r -> [r ByteString]
--getFields fs r = map (flip getField r) fs


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

data Scanner = Scanner ByteString

newScanner :: FilePath -> IO Scanner
newScanner fp = Scanner <$>  B.readFile fp

nextLine :: Identity Scanner -> (Identity ByteString, Identity Scanner)
nextLine (Identity (Scanner bs)) =
  let (fs, rs) = BC.span (/= '\n') bs
  in (Identity fs, Identity (Scanner (BC.tail rs)))

-- As |span| is not stage aware, it is more dynamic an necessary. Splitting
-- the implementation up means that we can skip over entire rows if
-- necessary in the generated code.
_nextLineCode :: Code Scanner -> (Code ByteString, Code Scanner)
_nextLineCode scanner =
  let fs = Code [|| let (Scanner s) = $$(runCode scanner) in BC.takeWhile (/= '\n') s ||]
      ts = Code [|| let (Scanner s) = $$(runCode scanner) in Scanner (BC.tail (BC.dropWhile (/= '\n') s)) ||]
  in (fs, ts)



hasNext :: Scanner -> Bool
hasNext (Scanner bs) = bs /= ""

while ::
  (Ops r, Monoid m) =>
  r (t -> Bool) -> r ((t -> IO m) -> t -> IO m) -> r (t -> IO m)
while k b = _fix (\r -> _lam $ \rs -> _when (k <*> rs) (b <*> r <*> rs))

whenM :: Monoid m => Bool -> m -> m
whenM b act = if b then act else mempty

type family ResT r1 r2 :: * -> * where
  ResT Identity Code = Code
  ResT Code Identity = Code
  ResT Identity r = r

processCSV :: forall m r r1 . (Monoid m, O r1 r)
           => Schema -> FilePath -> (r1 (Record r) -> (ResT r1 r) (IO m)) -> (ResT r1 r) (IO m)
processCSV ss f yld =
        _newScanner f `_bind` rows ss
  where
    rows :: Schema -> (ResT r1 r) (Scanner -> (IO m))
    rows sch = do
      while (_lam _hasNext)
            (_lam $ \r -> _lam $ \rs ->
              (let (hs, ts) = _nextLine rs
                   rec = _mkRecord (parseRow sch hs) (pure sch)
              in yld rec  >>> (r <*> ts))
              )

    -- We can't use the standard |BC.split| function here because
    -- we we statically know how far we will unroll. The result is then
    -- more static as we can do things like drop certain fields if we
    -- perform a projection.
    parseRow :: Schema -> (ResT r1 r) ByteString -> r1 [r ByteString]
    parseRow [] _ = _empty
    parseRow [_] b =
      weaken (take_while '\n' b) `_cons` _empty
    parseRow (_:ss') b =
      let new = tail_dropwhile ',' b
          rs = parseRow ss' new
      in weaken (take_while ',' b)  `_cons` rs

class Weaken r1 r where
  weaken :: (ResT r1 r) a -> r1 (r a)

instance Weaken Code Identity where
  weaken (Code c) = Code [|| Identity $$c ||]

instance Weaken Identity i where
  weaken i = Identity i



printFields :: Ops r => Fields r -> r Res
printFields [] = _empty
printFields [x] = _putStr x >>> _putStr (pure "\n")
printFields (x:xs) =
  _putStr x >>> _putStr (pure ",") >>> printFields xs

class Collapse r1 r2 where
  c :: r1 (r2 a) -> (ResT r1 r2) a

instance Collapse Identity r where
  c (Identity a) = a

instance Collapse Code Identity where
  c (Code c) = Code [|| runIdentity $$c ||]

type O r1 r = (Collapse r1 r
                          , Ops r
                          , Ops (ResT r1 r)
                          , Ops r1
                          , Weaken r1 r)

evalPred :: forall r1 r . (O r1 r)
         => Predicate -> r1 (Record r) -> (ResT r1 r) Bool
evalPred  predicate rec =
  case predicate of
    Eq a b -> eq (evalRef a rec) (evalRef b rec)
    Ne a b -> neq (evalRef a rec) (evalRef b rec)

evalRef :: (Collapse r1 r
           , Ops r
           , Ops (ResT r1 r)
           , Ops r1 ) => Ref -> r1 (Record r) -> (ResT r1 r) ByteString
evalRef (Value a) _ = pure a
evalRef (Field name) r = c $ getField (pure name) r


class O r1 r => ListEq (s :: Symbol) r r1 where
  list_eq :: (Eq a) => r1 [r a] -> r1 [r a] -> (ResT r1 r) Bool

class O r1 r => Restrict (s :: Symbol) r r1 where
  restrict :: Ops r => r1 (Record r) -> Schema -> Schema -> r1 (Record r)

instance Ops r => Restrict "unrolled" r Identity where
  restrict r newSchema parentSchema =
    let ns = map Identity parentSchema
        nfs = sequence $ map (flip getField r) ns
    in Record <$> nfs <*> Identity newSchema
--    Record <$> (map (flip getField r) parentSchema) <*> _

instance Ops r => ListEq "unrolled" r Identity where
  list_eq :: (Eq a) => Identity [r a] -> Identity [r a] -> r Bool
  list_eq (Identity []) (Identity []) = pure True
  list_eq (Identity (v:vs)) (Identity (v1:v1s))
    = _if (eq v v1) (list_eq @"unrolled" (Identity vs) (Identity v1s)) (pure False)
  list_eq _ _ = pure False

instance ListEq "recursive" Identity Identity where
  list_eq :: (Eq a) => Identity [Identity a] -> Identity [Identity a] -> Identity Bool
  list_eq xs ys = Identity (xs == ys)

pull :: [Code a] -> Code [a]
pull [] = Code [|| [] ||]
pull (x:xs) = Code [|| $$(runCode x) : $$(runCode $ pull xs) ||]

instance ListEq "recursive" Code Identity where
  list_eq :: (Eq a) => Identity [Code a] -> Identity [Code a] -> Code Bool
  list_eq (Identity xs) (Identity ys) = Code [|| $$(runCode $ pull xs) == $$(runCode $ pull ys) ||]


-- if r = Code then r1 = Identity
-- if r = Identity then r1 = Code or Identity
-- Having two type parameters means that we can either choose to use a
-- continuation which statically knows the argument (r1 = Identity) or not.
execOp :: forall restrict le r r1 m . (
                                        Restrict restrict r r1
                                      , ListEq le r r1
                                      , Monoid m
                                      , Ops r
                                      , Ops r1)
                                      => Operator
                                      -> (r1 (Record r) -> (ResT r1 r) (IO m))
                                      -> (ResT r1 r) (IO m)
execOp op yld =
  case op of
    Scan file sch ->
      processCSV sch file yld
    Filter predicate parent -> execOp @restrict @le parent
      (\rec -> _if (evalPred @r1 @r predicate rec) (yld rec) _empty )
    Project newSchema parentSchema parent ->
      execOp @restrict @le parent (\rec -> yld (restrict @restrict rec newSchema parentSchema ))
    Join left right ->
      execOp @restrict @le left (\rec -> execOp @restrict @le right (\rec' ->
        let k1 = _schema rec
            k2 = _fields rec
            keys = _intersect (_schema rec) (_schema rec')
            leq = list_eq @le @r @r1 @ByteString k2 (_fields rec')
        in _when leq
               (yld (_mkRecord (_fields rec >>> _fields rec')
                               (_schema rec >>> _schema rec')
                       ))))

_fields r = _case_record r (_lam $ \fs -> _lam $ \_ -> fs)
_schema r = _case_record r (_lam $ \_  -> _lam $ \ss -> ss)

execOpU :: forall r r1 m . (Monoid m, O r r1
                           , Restrict "unrolled" r r1
                           , ListEq "unrolled" r r1)
        => Operator
        -> (r1 (Record r) -> (ResT r1 r) (IO m))
        -> (ResT r1 r) (IO m)
execOpU = execOp @"unrolled" @"unrolled"

runQuery :: Operator -> Q (TExp Res)
runQuery q = runCode $ execOpU q (printFields . fields . runIdentity)

runQueryL :: Operator -> Q (TExp (IO [Record Identity]))
runQueryL q = runCode $ execOpU q (\r -> Code [|| return (return $$(runCode $ spill (runIdentity r))) ||])

spill :: Record Code -> Code (Record Identity)
spill (Record rs ss) = Code [|| Record $$(runCode $ spill2 rs) ss ||]

spill2 :: [Code ByteString] -> Code [Identity ByteString]
spill2 [] = Code [|| [] ||]
spill2 (x:xs) = Code [|| (Identity $$(runCode x)) : $$(runCode $ spill2 xs) ||]

runQueryUnstaged :: Operator -> Res
runQueryUnstaged q = runIdentity (execOpU q (printFields . fields . runIdentity))

runQueryUnstagedL :: Operator -> IO [Record Identity]
runQueryUnstagedL q = runIdentity (execOpU @Identity @Identity q (return . return . return . runIdentity))

test :: IO ()
test = do
--  processCSV "data/test.csv" (print . getField "name")
  --expr <- runQ $ unTypeQ  $ runCode $ execOpU query (printFields . fields)
--  expr <- runQ $ unTypeQ  $ power
  --putStrLn $ pprint expr
  print ""


bsToExp :: B.ByteString -> Q Exp
bsToExp bs = do
    helper <- [| stringToBs |]
    let chars = BC.unpack . BC.tail . (BC.dropWhile (/= '\n')) $ bs
    return $! AppE helper $! LitE $! StringL chars

stringToBs :: String -> B.ByteString
stringToBs = BC.pack

