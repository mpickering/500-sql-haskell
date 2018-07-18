{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE OverloadedStrings #-}
--{-# OPTIONS_GHC -ddump-splices #-}
module Test where

import Compiler
import qualified LMS as L

main = do
  ($$(runQuery query))
  ($$(runQuery query2))

  putStrLn ($$(L.runQuery L.query))
  putStrLn ($$(L.runQuery L.query2))

  putStrLn (L.runQueryUnstaged L.query)
  putStrLn (L.runQueryUnstaged L.query2)

