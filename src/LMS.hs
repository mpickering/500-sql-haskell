{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE ScopedTypeVariables #-}
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

class Ops r where
  eq :: Eq a => r a -> r a -> r Bool
  neq :: Eq a => r a -> r a -> r Bool
  bc_split :: r Char -> r ByteString -> r [ByteString]
  tail_dropwhile :: Char -> r ByteString -> r ByteString
  take_while :: Char -> r ByteString -> r ByteString
  _show :: Show a => r a -> r String
  _if :: r Bool -> r a -> r a -> r a
  _caseString :: r ByteString -> r a -> r a -> r a
  _fix :: (r a -> r a) -> r a
  _lam :: (r a -> r b) -> r (a -> b)
  (+++) :: r String -> r String -> r String


  _embedFile :: FilePath -> r ByteString
  pure :: Lift a => a -> r a
  (<*>) :: r (a -> b) -> r a -> r b

infixl 4 <*>

newtype Code a = Code (Q (TExp a))

instance Ops Code where
  eq (Code e1) (Code e2) = Code [|| $$e1 == $$e2 ||]
  neq (Code e1) (Code e2) = Code [|| $$e1 /= $$e2 ||]
  bc_split (Code e1) (Code e2) = Code [|| BC.split $$e1 $$e2 ||]
  _show (Code a) = Code [|| show $$a ||]
  _if (Code a) (Code b) (Code c) = Code [|| if $$a then $$b else $$c ||]
  _caseString (Code a) (Code b) (Code c) =
    Code [|| case $$a of
                "" -> $$b
                _  -> $$c ||]
  (+++) (Code a) (Code b) = Code [|| $$a ++ $$b ||]
  tail_dropwhile c (Code b) = Code [|| BC.tail (BC.dropWhile (/= c) $$b) ||]
  take_while c (Code b) = Code [|| BC.takeWhile (/= c) $$b ||]
  _fix f = Code [|| fix (\a -> $$(runCode $ f (Code [||a||]))) ||]

  _lam f = Code $ [|| \a ->  $$(runCode $ f (Code [|| a ||]))  ||]

  _embedFile = embedFileT

  pure = Code . unsafeTExpCoerce . lift
  (Code f) <*> (Code a) = Code [|| $$f $$a ||]


instance Ops Identity where
  eq = liftA2 (==)
  neq = liftA2 (/=)
  bc_split = liftA2 (BC.split)
  tail_dropwhile c = fmap (BC.tail . (BC.dropWhile (/= c)))
  take_while c = fmap (BC.takeWhile (/= c))
  _show = fmap show
  _if (Identity b) (Identity c1) (Identity c2) = Identity (if b then c1 else c2)
  _caseString (Identity b) (Identity c1) (Identity c2) =
    Identity (case b of
                "" -> c1
                _  -> c2)
  _fix = fix
  _lam f = Identity (\a -> runIdentity (f (Identity a)))
  (+++) = liftA2 (++)


  _embedFile = Identity . unsafePerformIO . BC.readFile

  pure = Identity
  (<*>) (Identity a1) (Identity a2) = Identity (a1 a2)



runCode :: Code a -> Q (TExp a)
runCode (Code a) = a



type Fields r = [r ByteString]
type Schema = [ByteString]
type Table = FilePath

data Record r = Record { fields :: Fields r, schema :: Schema }

getField :: Ops r => ByteString -> Record r -> r ByteString
getField field (Record fs sch) =
  let i = fromJust (elemIndex field sch)
  in  (fs !! i)


data Operator = Scan FilePath Schema | Print Operator | Project Schema Schema Operator
              | Filter Predicate Operator | Join Operator Operator deriving Show

query, query2 :: Operator
query = Project ["age"] ["age"] (Filter (Eq (Value "john") (Field "name")) (Scan "data/test.csv" ["name", "age"]))

query2 = Project ["name"] ["name"] (Filter (Eq (Value "34") (Field "age")) (Scan "data/test.csv" ["name", "age"]))


data Predicate = Eq Ref Ref | Ne Ref Ref deriving Show

data Ref = Field ByteString | Value ByteString deriving Show

type QTExp a = Code a

fix :: (a -> a) -> a
fix f = let x = f x in x

parseRow' :: Ops r => Schema -> r (ByteString -> ByteString)
parseRow' [] = _lam id
parseRow' [_] = _lam (\bs -> tail_dropwhile '\n' bs)
parseRow' (_:ss) = _lam (\bs -> parseRow' ss <*> tail_dropwhile ',' bs)

processCSV :: forall r . Ops r => Schema -> r ByteString -> (Record r -> r String) -> r String
processCSV ss bs yld =
  rows ss <*> bs
  where
    rows :: Ops r => Schema -> r (ByteString -> String)
    rows sch = do
        _fix (\r -> _lam (\rs ->
          _caseString rs (pure "")
                ((let (_, fs) = parseRow sch rs
                  in yld (Record fs sch)
                  ) +++ (r <*>) (parseRow' sch <*> rs) )))


    parseRow :: Ops r => Schema -> r ByteString -> (r ByteString, [r ByteString])
    parseRow [] b = (b, [])
    parseRow [_] b =
      ( tail_dropwhile ',' b
      , [take_while '\n' b])

    parseRow (_:ss') b =
      let new = tail_dropwhile ',' b
          (final, rs) = parseRow ss' new
      in (final, take_while ',' b : rs)



printFields :: Ops r => Fields r -> r String
printFields [] = pure "\n"
printFields [x] = _show x +++ pure "\n"
printFields (x:xs) =
  _show x +++ printFields xs

evalPred :: Ops r => Predicate -> Record r -> r Bool
evalPred  predicate rec =
  case predicate of
    Eq a b -> eq (evalRef a rec) (evalRef b rec)
    Ne a b -> neq (evalRef a rec) (evalRef b rec)

evalRef :: Ops r => Ref -> Record r -> r ByteString
evalRef (Value a) _ = pure a
evalRef (Field name) r = getField name r


restrict :: Ops r => Record r -> Schema -> Schema -> Record r
restrict r newSchema parentSchema =
  Record (map (flip getField r) parentSchema) newSchema


execOp :: Ops r => Operator -> (Record r -> r String) -> r String
execOp op yld =
  case op of
    Scan file sch ->
      processCSV sch (_embedFile file) yld
    Print p       -> execOp p (printFields . fields)
    Filter predicate parent -> execOp parent
      (\rec -> _if (evalPred predicate rec) (yld rec) (pure "") )
    Project newSchema parentSchema parent ->
      execOp parent (\rec -> yld (restrict rec newSchema parentSchema ))
    Join left right ->
      execOp left (\rec -> execOp right (\rec' ->
        let keys = schema rec `intersect` schema rec'
        -- TODO: Fix
        in yld (Record (fields rec ++ fields rec')
                       (schema rec ++ schema rec'))))

runQuery :: Operator -> Q (TExp String)
runQuery q = runCode $ execOp (Print q) (\_ -> pure "")

runQueryUnstaged :: Operator -> String
runQueryUnstaged q = runIdentity (execOp (Print q) (\_ -> ""))

test :: IO ()
test = do
--  processCSV "data/test.csv" (print . getField "name")
  expr <- runQ $ unTypeQ  $ runCode $ execOp (Print query) (\_ -> Code [|| "" ||])
--  expr <- runQ $ unTypeQ  $ power
  putStrLn $ pprint expr


embedFileT :: FilePath -> Code ByteString
embedFileT = Code . unsafeTExpCoerce . embedFile

embedFile :: FilePath -> Q Exp
embedFile fp = (runIO $ B.readFile fp) >>= bsToExp

bsToExp :: B.ByteString -> Q Exp
bsToExp bs = do
    helper <- [| stringToBs |]
    let chars = BC.unpack bs
    return $! AppE helper $! LitE $! StringL chars

stringToBs :: String -> B.ByteString
stringToBs = BC.pack

