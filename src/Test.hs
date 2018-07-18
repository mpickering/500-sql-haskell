{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE OverloadedStrings #-}
--{-# OPTIONS_GHC -ddump-splices #-}
module Test where

import Compiler
import qualified LMS as L

main = do
  ($$(runQuery query))
  ($$(runQuery query2))

  ($$(L.runQuery L.query))
  ($$(L.runQuery L.query2))

  (L.runQueryUnstaged L.query)
  (L.runQueryUnstaged L.query2)

