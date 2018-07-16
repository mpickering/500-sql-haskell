{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE NoImplicitPrelude #-}
module LMS where

import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as BC
import Data.ByteString (ByteString)
import qualified Scanner as S
import Control.Monad
import Data.List
import Data.Maybe
import Language.Haskell.TH
import Language.Haskell.TH
import Language.Haskell.TH.Syntax
import Prelude hiding (Applicative(..))
import Instances.TH.Lift
--import Data.FileEmbed
import Debug.Trace

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
  pure :: Lift a => a -> r a
  (<*>) :: r (a -> b) -> r a -> r b

infixl 4 <*>


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

  pure = Code . unsafeTExpCoerce . lift
  (Code f) <*> (Code a) = Code [|| $$f $$a ||]

newtype Code a = Code (Q (TExp a))

runCode :: Code a -> Q (TExp a)
runCode (Code a) = a



type Fields = [QTExp ByteString]
type Schema = [ByteString]
type Table = FilePath

data Record = Record { fields :: Fields, schema :: Schema }

getField :: ByteString -> Record -> QTExp ByteString
getField field (Record fs sch) =
  let i = fromJust (elemIndex field sch)
  in  (fs !! i)


data Operator = Scan FilePath Schema | Print Operator | Project Schema Schema Operator
              | Filter Predicate Operator | Join Operator Operator deriving Show

query = Project ["age"] ["age"] (Filter (Eq (Value "john") (Field "name")) (Scan "data/test.csv" ["name", "age"]))

query2 = Project ["name"] ["name"] (Filter (Eq (Value "34") (Field "age")) (Scan "data/test.csv" ["name", "age"]))


data Predicate = Eq Ref Ref | Ne Ref Ref deriving Show

data Ref = Field ByteString | Value ByteString deriving Show

type QTExp a = Code a

fix :: (a -> a) -> a
fix f = let x = f x in x

parseRow' :: Schema -> QTExp (ByteString -> ByteString)
parseRow' [] = _lam id
parseRow' [n] = _lam (\bs -> tail_dropwhile '\n' bs)
parseRow' (_:ss) = _lam (\bs -> parseRow' ss <*> tail_dropwhile ',' bs)

processCSV :: Schema -> Code ByteString -> (Record -> Code String) -> Code String
processCSV ss bs yld =
  rows ss <*> bs
  where
    rows :: Schema -> QTExp (ByteString -> String)
    rows schema = do
        _fix (\r -> _lam (\rs ->
          _caseString rs (pure "")
                ((let (_, fields) = parseRow schema rs
                      head = yld (Record fields schema)
                 in head) +++ (r <*>) (parseRow' schema <*> rs) )))


    parseRow :: Schema -> QTExp ByteString -> (QTExp ByteString, [QTExp ByteString])
    parseRow [] b = (b, [])
    parseRow [n] b =
      ( tail_dropwhile ',' b
      , [take_while '\n' b])

    parseRow (_:ss) b =
      let new = tail_dropwhile ',' b
          (final, rs) = parseRow ss new
      in (final, take_while ',' b : rs)



printFields :: Fields -> QTExp String
printFields [] = pure "\n"
printFields [x] = _show x +++ pure "\n"
printFields (x:xs) =
  _show x +++ printFields xs

evalPred :: Predicate -> Record -> Code Bool
evalPred  pred rec =
  case pred of
    Eq a b -> eq (evalRef a rec) (evalRef b rec)
    Ne a b -> neq (evalRef a rec) (evalRef b rec)

evalRef :: Ref -> Record -> Code ByteString
evalRef (Value a) _ = pure a
evalRef (Field name) r = getField name r


restrict :: Record -> Schema -> Schema -> Record
restrict r newSchema parentSchema =
  Record (map (flip getField r) parentSchema) newSchema


execOp :: Operator -> (Record -> Code String) -> Code String
execOp op yld = traceShow ("execOp", op) $
  case op of
    Scan file schema ->
      processCSV schema (embedFileT file) yld
    Print p       -> execOp p (printFields . fields)
    Filter pred parent -> execOp parent
      (\rec -> _if (evalPred pred rec) (yld rec) (pure "") )
    Project newSchema parentSchema parent ->
      execOp parent (\rec -> yld (restrict rec newSchema parentSchema ))
    Join left right ->
      execOp left (\rec -> execOp right (\rec' ->
        let keys = schema rec `intersect` schema rec'
        in yld (Record (fields rec ++ fields rec')
                       (schema rec ++ schema rec'))))

runQuery :: Operator -> Q (TExp String)
runQuery q = runCode $ execOp (Print q) (\_ -> pure "")

test = do
--  processCSV "data/test.csv" (print . getField "name")
  expr <- runQ $ unTypeQ  $ runCode $ execOp (Print query) (\_ -> Code [|| "" ||])
--  expr <- runQ $ unTypeQ  $ power
  putStrLn $ pprint expr


embedFileT :: FilePath -> QTExp ByteString
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

